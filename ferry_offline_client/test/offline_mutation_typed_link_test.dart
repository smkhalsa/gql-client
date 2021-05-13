// ignore_for_file: unawaited_futures
import 'dart:async';
import 'package:ferry/typed_links.dart';
import 'package:test/test.dart';
import 'package:hive/hive.dart';
import 'package:rxdart/rxdart.dart';
import 'package:ferry_exec/ferry_exec.dart';
import 'package:async/async.dart';

import 'package:ferry_offline_client/ferry_offline_client.dart';
import 'package:ferry_offline_client/src/offline_mutation_typed_link.dart';

import 'package:ferry_test_graphql/schema/serializers.gql.dart';
import 'package:ferry_test_graphql/mutations/variables/create_review.req.gql.dart';
import 'package:ferry_test_graphql/mutations/variables/create_review.data.gql.dart';
import 'package:ferry_test_graphql/schema/schema.schema.gql.dart';

final req1 = GCreateReviewReq(
  (b) => b
    ..requestId = 'test1'
    ..vars.review.stars = 5
    ..vars.episode = GEpisode.NEWHOPE
    ..vars.review.commentary = 'Amazing!!!',
);

final req2 = GCreateReviewReq(
  (b) => b
    ..requestId = 'test2'
    ..vars.review.stars = 3
    ..vars.episode = GEpisode.JEDI
    ..vars.review.commentary = 'This was meh',
);

final req3 = GCreateReviewReq(
  (b) => b
    ..requestId = 'test3'
    ..vars.review.stars = 4
    ..vars.episode = GEpisode.NEWHOPE
    ..vars.review.commentary = 'This was the beginning',
);

final req4 = GCreateReviewReq(
  (b) => b
    ..requestId = 'test4'
    ..vars.review.stars = 5
    ..vars.episode = GEpisode.EMPIRE
    ..vars.review.commentary = 'This was sooo good',
);

class MockException extends LinkException {
  MockException(originalException) : super(originalException);
}

final data = {
  req1.requestId: OperationResponse(
    operationRequest: req1,
    dataSource: DataSource.Link,
    data: GCreateReviewData(
      (b) => b
        ..createReview.id = '120'
        ..createReview.stars = 5
        ..createReview.episode = GEpisode.NEWHOPE
        ..createReview.commentary = 'Amazing!!!',
    ),
  ),
  req2.requestId: OperationResponse(
    operationRequest: req2,
    dataSource: DataSource.Link,
    data: GCreateReviewData(
      (b) => b
        ..createReview.id = '121'
        ..createReview.stars = 3
        ..createReview.episode = GEpisode.JEDI
        ..createReview.commentary = 'This was meh',
    ),
  ),
  req3.requestId: OperationResponse(
    operationRequest: req3,
    dataSource: DataSource.Link,
    data: GCreateReviewData(
      (b) => b
        ..createReview.id = '121'
        ..createReview.stars = 3
        ..createReview.episode = GEpisode.JEDI
        ..createReview.commentary = 'This was meh',
    ),
  ),
  req4.requestId: OperationResponse(
    operationRequest: req4,
    dataSource: DataSource.Link,
    data: GCreateReviewData(
      (b) => b
        ..createReview.id = '120'
        ..createReview.stars = 3
        ..createReview.episode = GEpisode.JEDI
        ..createReview.commentary = 'This was meh',
    ),
  )
};

class MockClient extends OfflineClient {
  @override
  final StreamController<OperationRequest> requestController;
  final TypedLink _link;

  MockClient(this.requestController, OfflineMutationTypedLink offlineLink)
      : _link = createLink(requestController, offlineLink);

  static TypedLink createLink(
    StreamController<OperationRequest> requestController,
    OfflineMutationTypedLink offlineLink,
  ) {
    final controllerLink = TypedLink.function(
      <TData, TVars>(request, [forward]) => requestController.stream
          .whereType<OperationRequest<TData, TVars>>()
          .where(
            (req) => req.requestId == null
                ? req == request
                : req.requestId == request.requestId,
          )
          .switchMap(
            (req) => forward(request),
          )
          .doOnListen(
        () {
          if (request.executeOnListen) {
            scheduleMicrotask(() => requestController.add(request));
          }
        },
      ),
    );
    final terminatingLink = TypedLink.function(
      <TData, TVars>(request, [forward]) {
        return Stream.value(
          data[request.requestId] as OperationResponse<TData, TVars>,
        );
      },
    );
    // offline mutation link must go between controller link and terminating link
    return TypedLink.from([
      controllerLink,
      offlineLink,
      terminatingLink,
    ]);
  }

  @override
  Stream<OperationResponse<TData, TVars>> request<TData, TVars>(req,
      [forward]) {
    return _link.request(req, forward);
  }
}

void main() {
  Hive.init('./test/__hive_data__');

  MockClient client;
  Box<Map<String, dynamic>> box;
  OfflineMutationTypedLink offlineLink;

  setUp(() async {
    box = await Hive.openBox<Map<String, dynamic>>('mutation_queue');
    await box.clear();

    final requestController = StreamController<OperationRequest>.broadcast();
    final cache = Cache();

    offlineLink = OfflineMutationTypedLink(
      mutationQueueBox: box,
      serializers: serializers,
      cache: cache,
      requestController: requestController,
    );

    client = MockClient(requestController, offlineLink);
    offlineLink.client = client;
  });

  test('mutations get enqueued and dequeued', () async {
    // online, initial request is executed
    offlineLink.connected = true;

    final queue = StreamQueue(client.request(req1));

    expect((await queue.next).data, equals(data[req1.requestId].data));

    // client goes offline, subsequent request is queued
    offlineLink.connected = false;
    client.requestController.add(req1);
    queue.hasNext;
    await Future.delayed(Duration.zero);

    expect(box.keys.length, equals(1));

    // client comes back online, queued request is executed
    offlineLink.connected = true;

    expect((await queue.next).data, equals(data[req1.requestId].data));

    expect(box.keys.length, equals(0));
  });

  test('ensure mutations are completed sequentially', () async {
    offlineLink.connected = false;
    expect(box.keys.length, 0);
    client.request(req1).first;
    client.request(req2).first;
    client.request(req3).first;
    client.request(req4).first;
    await client.requestController.stream.take(4).toList();
    expect(box.keys.length, 4);
    offlineLink.connected = true;
    await expectLater(
      client.requestController.stream,
      emitsInOrder([
        isA<GCreateReviewReq>().having(
          (req) => req.requestId,
          'requestId',
          equals(req1.requestId),
        ),
        isA<GCreateReviewReq>().having(
          (req) => req.requestId,
          'requestId',
          equals(req2.requestId),
        ),
        isA<GCreateReviewReq>().having(
          (req) => req.requestId,
          'requestId',
          equals(req3.requestId),
        ),
        isA<GCreateReviewReq>().having(
          (req) => req.requestId,
          'requestId',
          equals(req4.requestId),
        ),
      ]),
    );
  });
}
