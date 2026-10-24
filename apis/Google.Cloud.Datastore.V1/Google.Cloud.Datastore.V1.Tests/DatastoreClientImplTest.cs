// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax.Grpc;
using Google.Protobuf;
using Grpc.Core;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.Datastore.V1.Tests
{
    public class DatastoreClientImplTest
    {
        private const string SampleProjectId = "proj";
        private static readonly ByteString s_sampleTransaction = ByteString.CopyFromUtf8("tx1");

        [Theory]
        [InlineData(true, "true")]
        [InlineData(false, "false")]
        public void ApplyTransactionHeader(bool isTransaction, string expectedHeaderValue)
        {
            CallSettings settings = null;
            DatastoreClientImpl.ApplyTransactionHeader(ref settings, isTransaction);
            var metadata = new Metadata();
            settings.HeaderMutation?.Invoke(metadata);
            var entry = Assert.Single(metadata, e => e.Key == DatastoreClientImpl.TransactionHeader);
            Assert.Equal(expectedHeaderValue, entry.Value);
        }

        [Theory]
        [MemberData(nameof(AlwaysTransactionalTestData))]
        public void TransactionHeader_AlwaysTransactionalRpcs(Action<DatastoreClientImpl> rpcAction)
        {
            var invoker = new HeaderCapturingCallInvoker();
            var client = new DatastoreClientImpl(new Datastore.DatastoreClient(invoker), null, null);

            rpcAction(client);

            invoker.AssertTransactionHeader("true");
        }

        public static TheoryData<Action<DatastoreClientImpl>> AlwaysTransactionalTestData =>
            new TheoryData<Action<DatastoreClientImpl>>
            {
                client => client.BeginTransaction(new BeginTransactionRequest { ProjectId = SampleProjectId }),
                client => client.Rollback(new RollbackRequest { ProjectId = SampleProjectId, Transaction = s_sampleTransaction }),
            };

        [Theory]
        [MemberData(nameof(ConditionallyTransactionalTestData))]
        public void TransactionHeader_ConditionallyTransactionalRpcs(Action<DatastoreClientImpl> rpcAction, string expectedHeader)
        {
            var invoker = new HeaderCapturingCallInvoker();
            var client = new DatastoreClientImpl(new Datastore.DatastoreClient(invoker), null, null);

            rpcAction(client);

            invoker.AssertTransactionHeader(expectedHeader);
        }

        public static TheoryData<Action<DatastoreClientImpl>, string> ConditionallyTransactionalTestData =>
            new TheoryData<Action<DatastoreClientImpl>, string>
            {
                // Commit
                { client => client.Commit(new CommitRequest { ProjectId = SampleProjectId, Mode = CommitRequest.Types.Mode.NonTransactional }), "false" },
                { client => client.Commit(new CommitRequest { ProjectId = SampleProjectId, Mode = CommitRequest.Types.Mode.Transactional }), "true" },
                { client => client.Commit(new CommitRequest { ProjectId = SampleProjectId, Transaction = s_sampleTransaction }), "true" },
                { client => client.Commit(new CommitRequest { ProjectId = SampleProjectId, SingleUseTransaction = new TransactionOptions() }), "true" },

                // Lookup
                { client => client.Lookup(new LookupRequest { ProjectId = SampleProjectId }), "false" },
                { client => client.Lookup(new LookupRequest { ProjectId = SampleProjectId, ReadOptions = new ReadOptions { Transaction = s_sampleTransaction } }), "true" },
                { client => client.Lookup(new LookupRequest { ProjectId = SampleProjectId, ReadOptions = new ReadOptions { NewTransaction = new TransactionOptions() } }), "true" },

                // RunQuery
                { client => client.RunQuery(new RunQueryRequest { ProjectId = SampleProjectId }), "false" },
                { client => client.RunQuery(new RunQueryRequest { ProjectId = SampleProjectId, ReadOptions = new ReadOptions { Transaction = s_sampleTransaction } }), "true" },
                { client => client.RunQuery(new RunQueryRequest { ProjectId = SampleProjectId, ReadOptions = new ReadOptions { NewTransaction = new TransactionOptions() } }), "true" },

                // RunAggregationQuery
                { client => client.RunAggregationQuery(new RunAggregationQueryRequest { ProjectId = SampleProjectId }), "false" },
                { client => client.RunAggregationQuery(new RunAggregationQueryRequest { ProjectId = SampleProjectId, ReadOptions = new ReadOptions { Transaction = s_sampleTransaction } }), "true" },
                { client => client.RunAggregationQuery(new RunAggregationQueryRequest { ProjectId = SampleProjectId, ReadOptions = new ReadOptions { NewTransaction = new TransactionOptions() } }), "true" },
            };

        [Theory]
        [MemberData(nameof(NonTransactionalTestData))]
        public void TransactionHeader_NonTransactionalRpcs(Action<DatastoreClientImpl> rpcAction)
        {
            var invoker = new HeaderCapturingCallInvoker();
            var client = new DatastoreClientImpl(new Datastore.DatastoreClient(invoker), null, null);

            rpcAction(client);

            invoker.AssertTransactionHeader("false");
        }

        public static TheoryData<Action<DatastoreClientImpl>> NonTransactionalTestData =>
            new TheoryData<Action<DatastoreClientImpl>>
            {
                client => client.AllocateIds(new AllocateIdsRequest { ProjectId = SampleProjectId }),
                client => client.ReserveIds(new ReserveIdsRequest { ProjectId = SampleProjectId }),
            };

        private class HeaderCapturingCallInvoker : CallInvoker
        {
            public Metadata LastHeaders { get; private set; }

            public void AssertTransactionHeader(string expectedValue)
            {
                Assert.NotNull(LastHeaders);
                var entry = Assert.Single(LastHeaders, e => e.Key == DatastoreClientImpl.TransactionHeader);
                Assert.Equal(expectedValue, entry.Value);
                LastHeaders = null;
            }

            public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
            {
                LastHeaders = options.Headers;
                return new AsyncClientStreamingCall<TRequest, TResponse>(new FakeClientStreamWriter<TRequest>(), null, null, null, null, () => { });
            }

            public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options)
            {
                LastHeaders = options.Headers;
                return new AsyncDuplexStreamingCall<TRequest, TResponse>(new FakeClientStreamWriter<TRequest>(), null, null, null, null, () => { });
            }

            public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
            {
                LastHeaders = options.Headers;
                return new AsyncServerStreamingCall<TResponse>(null, null, null, null, () => { });
            }

            public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
            {
                LastHeaders = options.Headers;
                return new AsyncUnaryCall<TResponse>(Task.FromResult(Activator.CreateInstance<TResponse>()), null, null, null, () => { });
            }

            public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string host, CallOptions options, TRequest request)
            {
                LastHeaders = options.Headers;
                return Activator.CreateInstance<TResponse>();
            }

            private class FakeClientStreamWriter<T> : IClientStreamWriter<T>
            {
                public WriteOptions WriteOptions { get; set; }
                public Task CompleteAsync() => Task.CompletedTask;
                public Task WriteAsync(T message) => Task.CompletedTask;
            }
        }
    }
}
