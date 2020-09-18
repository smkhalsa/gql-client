import 'dart:async';
import 'package:ferry/ferry.dart';
import 'package:meta/meta.dart';
import 'package:hive/hive.dart';
import 'package:built_value/serializer.dart';
import 'package:gql/ast.dart';
import 'package:rxdart/rxdart.dart';
import 'package:normalize/utils.dart';

import 'package:ferry/src/operation_request.dart';
import 'package:ferry/src/operation_response.dart';
import 'package:ferry/src/plugins/plugin.dart';
import 'package:ferry/src/client.dart';

typedef LinkExceptionHandler<TData, TVars> = void Function(
  OperationResponse<TData, TVars> response,
  EventSink<OperationResponse<TData, TVars>> sink,
);

class OfflineMutationPlugin extends Plugin {
  /// A [hive] box where the mutation will be stored.
  final Box<Map<String, dynamic>> mutationQueueBox;

  /// The [Serializers] object generated by ferry's codegen.
  final Serializers serializers;

  /// A ferry [Client].
  final Client client;

  /// A callback used to customize behavior when a mutation execution results in a [LinkException].
  final LinkExceptionHandler linkExceptionHandler;

  /// A [StreamController] that keeps track of network connection status.
  ///
  /// You must update this value when the network status changes.
  final isConnectedController = BehaviorSubject<bool>.seeded(false);

  /// [OfflineMutationPlugin] can be used to maintain an Offline Mutation queue.
  ///
  /// This plugin is intended to be an example for how offline mutations can be
  /// handled with ferry but doesn't address all edge cases.
  ///
  /// Some limitations include:
  /// 1. If the client shuts down during an inflight mutation (i.e. after a
  /// mutation has been sent to the server but before a response has been received),
  /// the mutation is assumed to have completed succesfully.
  /// 2. When coming back online, all queued mutations are executed immediately
  /// in parallel, not serially. This could lead to unexpected behavior.
  OfflineMutationPlugin({
    @required this.mutationQueueBox,
    @required this.serializers,
    @required this.client,
    this.linkExceptionHandler,
  }) {
    isConnectedController.distinct().listen((isConnected) {
      if (isConnected) _handleOnConnect();
    });
  }

  void _handleOnConnect() => mutationQueueBox.values.forEach((json) {
        final OperationRequest req = serializers.deserialize(json);

        // Run unexecuted mutations
        client.requestController.add(req);
      });

  bool _isMutation(OperationRequest req) =>
      getOperationDefinition(
        req.operation.document,
        req.operation.operationName,
      ).type ==
      OperationType.mutation;

  @override
  StreamTransformer<OperationRequest<TData, TVars>,
          OperationRequest<TData, TVars>>
      buildRequestTransformer<TData, TVars>() => StreamTransformer.fromHandlers(
            handleData: (req, sink) {
              // Forward along any operations that aren't Mutations
              if (_isMutation(req) == false) return sink.add(req);

              // If the client is online, execute the mutation
              if (isConnectedController.value) return sink.add(req);

              // Save mutation to the queue
              mutationQueueBox.put(
                req.requestId,
                serializers.serialize(req),
              );

              // Add an optimistic patch to the cache, if necessary
              if (req.optimisticResponse != null) {
                client.cache.writeQuery(
                  req,
                  req.optimisticResponse,
                  optimistic: true,
                  requestId: req.requestId,
                );
              }
            },
          );

  @override
  StreamTransformer<OperationResponse<TData, TVars>,
          OperationResponse<TData, TVars>>
      buildResponseTransformer<TData, TVars>() =>
          StreamTransformer.fromHandlers(
            handleData: (res, sink) {
              // Forward along any responses for operations that aren't Mutations
              if (_isMutation(res.operationRequest) == false) {
                return sink.add(res);
              }

              if (res.linkException is ServerException &&
                  linkExceptionHandler != null) {
                return linkExceptionHandler(res, sink);
              }

              // Forward response and remove mutation from queue
              sink.add(res);
              mutationQueueBox.delete(res.operationRequest.requestId);
            },
          );

  @override
  Future<void> dispose() => isConnectedController.close();
}
