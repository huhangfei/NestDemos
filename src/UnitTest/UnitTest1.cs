using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest5Test;
using NestHelper;
using Nest;

namespace UnitTest
{
    /// <summary>
    /// 使用5.5操作5.5
    /// </summary>
    [TestClass]
    public class UnitTest1
    {
        private TestNest _testNest;
        private readonly List<Uri> _nodes = Helper.GetAllNodes();
        private static readonly string IndexName = "test_createindex_nest5";
        [TestInitialize]
        public void Init()
        {
            _testNest=new TestNest();
            Test5CreateIndex();
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5CreateElasticClient()
        {
            var client=_testNest.CreateElasticClient(_nodes);
            Assert.IsNotNull(client);
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5CreateIndex()
        {
            if (_testNest.IndexExists(IndexName))
                _testNest.DeleteIndex(IndexName);

            Assert.IsFalse(_testNest.IndexExists(IndexName));

            _testNest.CreateIndex(IndexName);

            Assert.IsTrue(_testNest.IndexExists(IndexName));
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5Map()
        {
            _testNest.Map(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5UpdateMap()
        {
            _testNest.UpdateMap(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5Index()
        {
            _testNest.Index(new TestModel5()
            {
                Id = 0,
                Name = "测试数据1",
                AddTime = DateTime.Now,
                CreateTime = DateTime.Now,
                Deleted = false,
                Dic = "测试内容测试内容测试内容测试内容测试内容测试内容测试内容测试内容",
                Dvalue = 222222222222.2222,
                Group = 1, 
                PassingRate = float.MaxValue,
                State = 1
            },IndexName );


            #region test data
            int index = 1;
            _testNest.Index(new TestModel5()
            {
                Id = index++,
                Deleted = false,
                Name = "油菜花",
                Dic = "一种植物，叫油菜花。",
                AddTime = DateTime.Now
            }, IndexName);

            _testNest.Index(new TestModel5()
            {
                Id = index++,
                Deleted = false,
                Name = "菜花",
                Dic = "一种蔬菜，叫菜花。",
                AddTime = DateTime.Now
            }, IndexName);

            _testNest.Index(new TestModel5()
            {
                Id = index++,
                Deleted = false,
                Name = "黄瓜",
                Dic = "一种蔬菜，叫油黄瓜。",
                AddTime = DateTime.Now
            }, IndexName);

            _testNest.Index(new TestModel5()
            {
                Id = index++,
                Deleted = false,
                Name = "苹果",
                Dic = "一种水果，叫苹果。",
                AddTime = DateTime.Now
            }, IndexName);

            #endregion
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5IndexMany()
        {
            var list1 = new List<TestModel5>();
            for (int i = 0; i < 500; i++)
            {
                list1.Add(new TestModel5()
                {
                    Id = i,
                    Name = "测试数据"+i,
                    AddTime = DateTime.Now,
                    CreateTime = DateTime.Now,
                    Deleted = false,
                    Dic = i + "测试内容测试内容测试内容测试内容测试内容测试内容测试内容测试内容",
                    Dvalue = 222222222222.2222,
                    Group = 1,
                    PassingRate = float.MaxValue,
                    State = 1
                });
            }
            _testNest.IndexMany(list1, IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5Get()
        {
            Test5DeleteIndex();

            Test5CreateIndex();

            Test5Index();

            Thread.Sleep(1000);

            var data= _testNest.Get(IndexName, 1);
            Assert.IsNotNull(data);
            Assert.IsTrue(data.Id==1);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5GetVersion()
        {
            _testNest.GetVersion(IndexName, 1);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5Update()
        {
            Test5DeleteIndex();

            Test5CreateIndex();

            Test5Index();

            Thread.Sleep(1000);

            _testNest.Update(IndexName, 1,new TestModel5() {Name = "ceshi"});
            var data = _testNest.Get(IndexName, 1);
            Assert.IsNotNull(data);
            Assert.IsTrue("ceshi"==data.Name);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5UpdateByWhere()
        {
            Test5DeleteIndex();

            Test5CreateIndex();

            Test5Index();

            Thread.Sleep(1000);

            _testNest.UpdateByWhere(IndexName);

            var data = _testNest.Get(IndexName, 2);
            Assert.IsNotNull(data);
            Assert.IsTrue("新的名字"==data.Name);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5Search()
        {
            _testNest.Search(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5SearchScanScroll()
        {
            _testNest.SearchScanScroll(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5SearchSetBoost()
        {
            _testNest.SearchSetBoost(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5SearchAfter()
        {
            _testNest.SearchAfter(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5ManyWhereSearch()
        {
            _testNest.ManyWhereSearch(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5FunctionScore()
        {
            _testNest.FunctionScore(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5BaseAggregation()
        {
            _testNest.BaseAggregation(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5Aggregations()
        {
            _testNest.Aggregations(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5AnalysisAggregationsResult()
        {
            _testNest.AnalysisAggregationsResult(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5Delete()
        {
            _testNest.Delete(IndexName,1);
        }
        
        /// <summary>
        /// match 关键词匹配
        /// 使用了默认分词 
        /// </summary>
        [TestMethod]
        public void Test5MatchQuery()
        {
            Test5DeleteIndex();

            Test5CreateIndex();

            Test5Index();

            var _client = _testNest.CreateElasticClient(_nodes);
            //立马刷新数据 否则有默认1s延迟，无法查询出来
            _client.Refresh(IndexName);

            var countResult = _client.Count<TestModel5>(s => s.Index(IndexName));

            Console.WriteLine(countResult.Count);


            var result = _client.Search<TestModel5>(s => s
                    .Index(IndexName)
                           .Query(q => q
                               .Bool(b => b
                                   .Must(m => m
                                   .Match(mt => mt
                                       .Field(fd => fd.Dic)
                                       .Query("油菜花")
                                        )
                                    )
                               )
                           )
                           .FielddataFields(fdf=>fdf.Fields(fd=>fd.Dic,fd=>fd.Name))//查看实际内容
                           .Size(10)
                   );

            Assert.AreEqual(3,result.Hits.Count);



            var result1 = _client.Search<TestModel5>(s => s
                    .Index(IndexName)
                           .Query(q => q
                               .Bool(b => b
                                   .Must(m => m
                                   .Match(mt => mt
                                       .Field(fd => fd.Dic)
                                       .Query("苹果")
                                        )
                                    )
                               )
                           )
                           .FielddataFields(fdf => fdf.Fields(fd => fd.Dic, fd => fd.Name))//查看实际内容
                           .Size(10)
                   );

            Assert.AreEqual(1,result1.Hits.Count);
        }


        /// <summary>
        /// QueryString 模糊匹配
        /// </summary>
        [TestMethod]
        public void Test5QueryString()
        {
            Test5DeleteIndex();

            Test5CreateIndex();

            Test5Index();

            var _client = _testNest.CreateElasticClient(_nodes);
            //立马刷新数据 否则有默认1s延迟，无法查询出来
            _client.Refresh(IndexName);

            var countResult = _client.Count<TestModel5>(s => s.Index(IndexName));

            Console.WriteLine(countResult.Count);

            var result = _client.Search<TestModel5>(s => s
                    .Index(IndexName)
                           .Query(q => q
                               .Bool(b => b
                                   .Must(m => m
                                       .Wildcard(qs=>qs.Field(fd=>fd.DicKeyword).Value("*油菜花*"))//Wildcard 要用keyword类型
                                    )
                               )
                           )
                           .FielddataFields(fdf => fdf.Fields(fd => fd.DicKeyword, fd => fd.Name))//查看实际内容
                           .Size(10)
                   );

            Assert.AreEqual(1,result.Hits.Count);


            var result1 = _client.Search<TestModel5>(s => s
                   .Index(IndexName)
                          .Query(q => q
                              .Bool(b => b
                                  .Must(m => m
                                      .Wildcard(qs => qs.Field(fd => fd.DicKeyword).Value("*蔬菜*"))//Wildcard 要用keyword类型
                                   )
                              )
                          )
                          .FielddataFields(fdf => fdf.Fields(fd => fd.DicKeyword, fd => fd.Name))//查看实际内容
                          .Size(10)
                  );

            Assert.AreEqual(2, result1.Hits.Count);

          
        }
         /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5DeleteByQuery()
        {
            _testNest.DeleteByQuery(IndexName);
        }
        #region TestCleanup
        /// <summary>
        /// 
        /// </summary>
        [TestCleanup]
        public void Test5DeleteIndex()
        {
            _testNest.DeleteIndex(IndexName);
        }

        #endregion
    }
}
