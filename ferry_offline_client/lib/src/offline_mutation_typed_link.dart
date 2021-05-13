import 'dart:async';
import 'package:async/async.dart';
import 'package:meta/meta.dart';
import 'package:hive/hive.dart';
import 'package:built_value/serializer.dart';
import 'package:gql/ast.dart';
import 'package:rxdart/rxdart.dart';
import 'package:normalize/utils.dart';
import 'package:ferry_exec/ferry_exec.dart';
import 'package:ferry_cache/ferry_cache.dart';
import 'package:ferry_offline_client/ferry_offline_client.dart';

export 'package:hive/hive.dart';
export 'package:ferry_cache/ferry_cache.dart';

typedef LinkExceptionHandler<TData, TVars> = void Function(
  OperationResponse<TData, TVars> response,
  EventSink<OperationResponse<TData, TVars>> sink,
);

/// Caches mutations in a `hive` box when offline and re-runs them when
/// re-connected.
///
/// This link must be between a `RequestControllerTypedLink` and the
/// terminating link.
class OfflineMutationTypedLink extends TypedLink {
  /// A [hive] box where the mutation will be stored.
  final Box<Map<dynamic, dynamic>> mutationQueueBox;

  /// The [Serializers] object generated by ferry's codegen.
  final Serializers serializers;

  final Cache cache;
  final StreamController<OperationRequest> requestController;
  final OfflineClientConfig config;

  OfflineClient client;
  bool _connected = false;

  bool get connected => _connected;

  set connected(bool isConnected) {
    _connected = isConnected;
    if (isConnected) _handleOnConnect();
  }

  /// [OfflineMutationPlugin] can be used to maintain an Offline Mutation queue.
  ///
  /// Requests are tried sequentially using the client any errors will cancel
  /// the queue processing
  OfflineMutationTypedLink({
    @required this.mutationQueueBox,
    @required this.serializers,
    @required this.cache,
    @required this.requestController,
    this.config = const OfflineClientConfig(),
  });

  void _handleOnConnect() async {
    final queue = StreamQueue(Stream.fromIterable(mutationQueueBox.values));
    while (await queue.hasNext) {
      final json = await queue.next;
      final req = serializers.deserialize(json);

      // Run unexecuted mutations
      await client.request(req).firstWhere(
            (rsp) =>
                rsp.dataSource == DataSource.Link &&
                rsp.operationRequest == req,
          );
    }
  }

  bool _isMutation(OperationRequest req) =>
      getOperationDefinition(
        req.operation.document,
        req.operation.operationName,
      ).type ==
      OperationType.mutation;

  @override
  Stream<OperationResponse<TData, TVars>> request<TData, TVars>(
    OperationRequest<TData, TVars> request, [
    NextTypedLink<TData, TVars> forward,
  ]) =>
      _handleRequest(request, forward).transform(_responseTransformer());

  Stream<OperationResponse<TData, TVars>> _handleRequest<TData, TVars>(
    OperationRequest<TData, TVars> request, [
    NextTypedLink<TData, TVars> forward,
  ]) {
    // Forward along any operations that aren't Mutations
    if (_isMutation(request) == false) return forward(request);
    // If the client is online, execute the mutation
    if (connected) return forward(request);

    // Save mutation to the queue
    mutationQueueBox.add(serializers.serialize(request));

    // Add an optimistic patch to the cache, if necessary
    if (request.optimisticResponse != null) {
      cache.writeQuery(
        request,
        request.optimisticResponse,
        optimisticRequest: !config.persistOptimisticResponse ? request : null,
      );
    }

    /// Don't forward
    return NeverStream();
  }

  StreamTransformer<OperationResponse<TData, TVars>,
          OperationResponse<TData, TVars>>
      _responseTransformer<TData, TVars>() => StreamTransformer.fromHandlers(
            handleData: (res, sink) {
              // Forward along any responses for operations that aren't Mutations
              // or any responses that are optimistic since these should not
              // cause mutations to be treated as executed
              if (_isMutation(res.operationRequest) == false ||
                  res.dataSource == DataSource.Optimistic) {
                return sink.add(res);
              }

              // if there are any response error pass them to the exception handler
              if (res.hasErrors) {
                config.linkExceptionHandler?.call(res, sink);
                // If specified do not allow any requests that have errors to be removed from
                // the queue as these need to be retried
                if (config.shouldDequeueRequest?.call(res) == true ||
                    !config.dequeueOnError) {
                  return sink.addError(res.linkException);
                }
              }

              // Forward response and remove mutation from queue
              sink.add(res);
              final queueKey = mutationQueueBox.values.toList().indexWhere(
                    (serialized) =>
                        res.operationRequest ==
                        serializers.deserialize(serialized),
                  );
              if (queueKey != -1) mutationQueueBox.deleteAt(queueKey);
            },
          );
}
