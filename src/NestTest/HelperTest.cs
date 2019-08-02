using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest;
using Nest7Demo;
using Shouldly;

namespace NestTest
{
    [TestClass]
    public class HelperTest
    {
        /// <summary>
        /// 测试创建客户端
        /// </summary>
        [TestMethod]
        public void TestCreateClient()
        {
            var c1 = EsHelper.CreateElasticClient(single: false);
            var c2 = EsHelper.CreateElasticClient(single: false);
            c1.Equals(c2).ShouldBeFalse();

            var c3= EsHelper.CreateElasticClient("Nodes", indexName:"book");
            var c4= EsHelper.CreateElasticClient("Nodes_1", indexName: "book");
            c3.Equals(c4).ShouldBeFalse();

            var c5 = EsHelper.CreateElasticClient(indexName: "book");
            var c6 = EsHelper.CreateElasticClient(indexName: "dog");
            c5.Equals(c6).ShouldBeFalse();
        }
        /// <summary>
        /// 测试单例客户端
        /// </summary>
        [TestMethod]
        public void TestCreateSingleClient()
        {
            ConcurrentQueue<IElasticClient> queue = new ConcurrentQueue<IElasticClient>();
            CancellationTokenSource tokenSource = new CancellationTokenSource();

            Action create = () => {
                while (!tokenSource.Token.IsCancellationRequested)
                {
                    var c = EsHelper.CreateElasticClient(single: true);
                    queue.Enqueue(c);
                }
            };

            Task[] tasks = new Task[20];
            for (int i = 0; i < 20; i++)
            {

                Task t =Task.Factory.StartNew(create, tokenSource.Token);
                tasks[i] = t;
            }

            Thread.Sleep(5000);

            tokenSource.Cancel();

            Console.WriteLine($"实例={queue.Count}");
            (queue.Count > 0).ShouldBeTrue();

            bool isSingle = true;
            Task.Factory.ContinueWhenAll(tasks, tks =>
            {
                IElasticClient clientFirst;
                queue.TryDequeue(out clientFirst);
                IElasticClient clientTemp;
                while (queue.TryDequeue(out clientTemp))
                {
                    if (!clientFirst.Equals(clientTemp)) {
                        isSingle = false;
                        break;
                    }
                }

            });

            isSingle.ShouldBeTrue();
        }

    }
}
