using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest2Test;
using Nest5Test;
using NestHelper;

namespace UnitTest2.x
{
    /// <summary>
    /// 使用2.3.4操作2.3.4
    /// </summary>
    [TestClass]
    public class UnitTest1
    {
        private TestNest _testNest;
        private readonly List<Uri> _nodes = Helper.GetAllNodes();
        private static readonly string IndexName = "test_createindex_nest3";
        [TestInitialize]
        public void Init()
        {
            _testNest = new TestNest();
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2CreateElasticClient()
        {
            var client = _testNest.CreateElasticClient(_nodes);
            Assert.IsNotNull(client);
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2IndexAction()
        {
            if(_testNest.IndexExists(IndexName))
                _testNest.DeleteIndex(IndexName);

            Assert.IsFalse(_testNest.IndexExists(IndexName));

            _testNest.CreateIndex(IndexName);

            Assert.IsTrue(_testNest.IndexExists(IndexName));
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2Map()
        {
            _testNest.Map(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2UpdateMap()
        {
            _testNest.UpdateMap(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2Index()
        {
            _testNest.Index(new TestModel2()
            {
                Id = 1,
                Name = "测试数据1",
                AddTime = DateTime.Now,
                CreateTime = DateTime.Now,
                Deleted = false,
                Dic = "测试内容测试内容测试内容测试内容测试内容测试内容测试内容测试内容",
                Dvalue = 222222222222.2222,
                Group = 1,
                PassingRate = float.MaxValue,
                State = 1
            }, IndexName);

        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2IndexMany()
        {
            var list1 = new List<TestModel2>();
            for (int i = 0; i < 500; i++)
            {
                list1.Add(new TestModel2()
                {
                    Id = i,
                    Name = "测试数据" + i,
                    AddTime = DateTime.Now,
                    CreateTime = DateTime.Now,
                    Deleted = false,
                    Dic = i + "测试内容测试内容测试内容测试内容测试内容测试内容测试内容测试内容",
                    Dvalue = 222222222222.2222,
                    Group = 1,
                    PassingRate = float.MaxValue,
                    State = i
                });
            }
            for (int i = 500; i < 520; i++)
            {
                list1.Add(new TestModel2()
                {
                    Id = i,
                    Name = "测试数据" + i,
                    AddTime = DateTime.Now,
                    CreateTime = DateTime.Now,
                    Deleted = false,
                    Dic = i + "测试内容测试内容测试内容测试内容测试内容测试内容测试内容测试内容",
                    Dvalue = 222222222222.2222,
                    Group = 1,
                    PassingRate = float.MaxValue,
                    State = 200
                });
            }
            _testNest.IndexMany(list1, IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2Get()
        {
            var data = _testNest.Get(IndexName, 1);
            Assert.IsNotNull(data);
            Assert.IsTrue(data.Id == 1);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2GetVersion()
        {
            _testNest.GetVersion(IndexName, 1);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2Update()
        {
            _testNest.Update(IndexName, 1, new TestModel2() { Name = "ceshi" });
            var data = _testNest.Get(IndexName, 1);
            Assert.IsNotNull(data);
            Assert.IsTrue("ceshi" == data.Name);
        }
        ///// <summary>
        ///// 
        ///// </summary>
        //[TestMethod]
        //public void Test2UpdateByWhere()
        //{
        //    Thread.Sleep(2000);
        //    _testNest.UpdateByWhere(IndexName);
        //    var data = _testNest.Get(IndexName, 2);
        //    Assert.IsNotNull(data);
        //    Assert.IsTrue("新的名字" == data.Name);
        //}
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2Search()
        {
            _testNest.Search(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2SearchScanScroll()
        {
            _testNest.SearchScanScroll(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2SearchSetBoost()
        {
            _testNest.SearchSetBoost(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2SearchAfter()
        {
            _testNest.SearchAfter(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2ManyWhereSearch()
        {
            _testNest.ManyWhereSearch(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2FunctionScore()
        {
            _testNest.FunctionScore(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2BaseAggregation()
        {
            _testNest.BaseAggregation(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2Aggregations()
        {
            _testNest.Aggregations(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2AnalysisAggregationsResult()
        {
            _testNest.AnalysisAggregationsResult(IndexName);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test2Delete()
        {
           
            _testNest.Delete(IndexName, 1);
        }
        ///// <summary>
        ///// 
        ///// </summary>
        //[TestMethod]
        //public void Test2DeleteByQuery()
        //{
        //    Thread.Sleep(2000);
        //    _testNest.DeleteByQuery(IndexName);
        //}

        [TestMethod]
        public void TestSearchRandom()
        {
            _testNest.SearchRandom(IndexName);
        }
    }
}
