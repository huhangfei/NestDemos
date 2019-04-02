using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest5Test;
using NestHelper;

namespace UnitTest5.x_do_2._3._4
{
    /// <summary>
    /// 使用5.5操作2.3.4
    /// </summary>
    [TestClass]
    public class UnitTest1
    {
        private TestNest2 _testNest;
        private readonly List<Uri> _nodes = Helper.GetAllNodes();
        private static readonly string IndexName = "test_createindex_nest5";
        [TestInitialize]
        public void Init()
        {
            _testNest = new TestNest2();
        }

        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5CreateElasticClient()
        {
            var client = _testNest.CreateElasticClient(_nodes);
            Assert.IsNotNull(client);
        }
        /// <summary>
        /// 
        /// </summary>
        [TestMethod]
        public void Test5IndexAction()
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
        public void Test5IndexMany()
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
            var data = _testNest.Get(IndexName, 1);
            Assert.IsNotNull(data);
            Assert.IsTrue(data.Id == 1);
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
            _testNest.Update(IndexName, 1, new TestModel2() { Name = "ceshi" });
            var data = _testNest.Get(IndexName, 1);
            Assert.IsNotNull(data);
            Assert.IsTrue("ceshi" == data.Name);
        }
        /// <summary>
        /// 
        /// </summary>
        //[TestMethod]
        //public void Test5UpdateByWhere()
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
            _testNest.Delete(IndexName, 1);
        }
        /// <summary>
        /// 
        /// </summary>
        //[TestMethod]
        //public void Test5DeleteByQuery()
        //{
        //    Thread.Sleep(2000);
        //    _testNest.DeleteByQuery(IndexName);
        //}
    }
}
