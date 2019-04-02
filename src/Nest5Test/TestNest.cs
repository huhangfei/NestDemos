using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Nest;
using NestHelper;

namespace Nest5Test
{
    /// <summary>
    /// 5.x操作
    /// </summary>
    public class TestNest
    {
        private readonly IElasticClient _client;
        public TestNest()
        {
            List<Uri> nodes = Helper.GetAllNodes();
            _client = CreateElasticClient(nodes);
        }
        public IConnectionPool CraeteConnectionPool(List<Uri> nodes)
        {

            //支持ping 说明能够发现节点的状态
            //支持嗅探 说明能够发现新的节点

            //应用于已知集群，请求时随机请求各个正常节点，支持ping 不支持嗅探
            IConnectionPool pool = new StaticConnectionPool(nodes);

            //IConnectionPool pool=new SingleNodeConnectionPool(nodes[0]);

            //可动态嗅探集群 ，随机请求 支持嗅探、ping
            //IConnectionPool pool = new SniffingConnectionPool(nodes);

            //选择一个可用节点作为请求主节点，支持ping 不支持嗅探
            //IConnectionPool pool = new StickyConnectionPool(nodes);

            //选择一个可用节点作为请求主节点，支持ping 支持嗅探
            //IConnectionPool pool=new StickySniffingConnectionPool(nodes);
            return pool;
        }
        public IElasticClient CreateElasticClient(List<Uri> nodes)
        {
            var pool = CraeteConnectionPool(nodes);

            var settings = new ConnectionSettings(pool);
            //验证 未开启
            //settings.BasicAuthentication("username", "password");
            //验证证书
            //settings.ClientCertificate("");
            //settings.ClientCertificates(new X509CertificateCollection());
            //settings.ServerCertificateValidationCallback();

            //开启 第一次使用时进行嗅探，需链接池支持
            //settings.SniffOnStartup(false);

            //链接最大并发数
            //settings.ConnectionLimit(80);
            //标记为死亡节点的超时时间
            //settings.DeadTimeout(new TimeSpan(10000));
            //settings.MaxDeadTimeout(new TimeSpan(10000));
            //最大重试次数
            //settings.MaximumRetries(5);
            //重试超时时间 默认是RequestTimeout
            //settings.MaxRetryTimeout(new TimeSpan(50000));
            //禁用代理自动检测
            //settings.DisableAutomaticProxyDetection(true);

            //禁用ping 第一次使用节点或使用被标记死亡的节点进行ping
            settings.DisablePing(false);
            //ping 超时设置
            //settings.PingTimeout(new TimeSpan(10000));
            //选择节点
            //settings.NodePredicate(node =>
            //{
            //    
            //        return true;
            //    
            //});
            //默认操作索引
            //settings.DefaultIndex("");
            //字段名规则 与model字段同名
            //settings.DefaultFieldNameInferrer(name => name);
            //根据Type 获取类型名
            //settings.DefaultTypeNameInferrer(name => name.Name);
            //请求超时设置
            //settings.RequestTimeout(new TimeSpan(10000));
            //调试信息
            settings.DisableDirectStreaming(true);
            //调试信息
            //settings.EnableDebugMode((apiCallDetails) =>
            //{
            //    //请求完成 返回 apiCallDetails

            //});
            //抛出异常
            settings.ThrowExceptions(true);
            //settings.OnRequestCompleted(apiCallDetails =>
            //{
            //    //请求完成 返回 apiCallDetails
            //});
            //settings.OnRequestDataCreated(requestData =>
            //{
            //    //请求的数据创建完成 返回请求的数据

            //});
            return new ElasticClient(settings);
        }
        public void CreateIndex(string indexName)
        {
            IIndexState indexState = new IndexState
            {
                Settings = new IndexSettings
                {
                    NumberOfReplicas = 1,//副本数
                    NumberOfShards = 2//分片数
                }
            };
            //可以设置一些特殊设置
            //indexState.Settings.Add("","");
            //创建index
            //var response=_client.CreateIndex(indexName, n => n.InitializeUsing(indexState));
            //创建index 设置分片/备份数 maping
            var response = _client.CreateIndex(indexName, n => n.InitializeUsing(indexState).Mappings(m => m.Map<TestModel5>(mp => mp.AutoMap())));
        }
        public void DeleteIndex(string indexName)
        {
            var resopnse = _client.DeleteIndex(indexName);
        }
        public bool IndexExists(string indexName)
        {
            var response = _client.IndexExists(indexName);

            return response.Exists;
        }
        public void Map(string indexName)
        {
            //根据对象类型自动映射
            var result = _client.Map<TestModel5>(m => m.Index(indexName).AutoMap());
            //手动指定
            //todo:5.x中是keyword、text
            //var result1 = _client.Map<TestModel5>(m => m.Properties(p => p.Keyword(s => s.Name(n => n.Name).Index(true))));//Keyword 类型
        }
        public void UpdateMap(string indexName)
        {
            //新增字段
            var result = _client.Map<TestModel5>(m => m
            .Index(indexName)
            .Properties(p => p
                    .Keyword(s => s
                            .Name("NewField")
                            .Index(true))
                    .Text(s=>s
                        .Name("NewFieldText")
                        .Index(false))
            )
            );
        }
        public void Index(TestModel5 data, string indexName)
        {
            //写入数据，指定索引
            _client.Index(data, s => s.Index(indexName));
            //指定索引、类型
            //_client.Index(data, s => s.Index(indexName).Type("TestModel5"));
        }
        public void IndexMany(List<TestModel5> datas, string indexName)
        {
            //写入数据，指定索引
            _client.IndexMany(datas, indexName);
            //指定索引、类型
            //_client.IndexMany(datas, indexName, "TestModel5");
        }
        public TestModel5 Get(string indexName, int id)
        {
            DocumentPath<TestModel5> path = new DocumentPath<TestModel5>(id);
            var response= _client.Get<TestModel5>(path,s=>s.Index(indexName));

            return response.Source;
        }
        public void Delete(string indexName,int id)
        {
            DocumentPath<TestModel5> deletePath = new DocumentPath<TestModel5>(id);
            var response = _client.Delete(deletePath, s => s.Index(indexName));
            // _client.Delete(deletePath, s => s.Index(indexName).Type(typeof(TestModel5)));
            //_client.Delete(deletePath, s => s.Index(indexName).Type("TestModel5"));

            //IDeleteRequest request = new DeleteRequest(indexName, typeof(TestModel5), id);
            //_client.Delete(request);
        }
        public void DeleteByQuery(string indexName)
        {
            //1.x中有 2.x中需要安装插件 5.x中又回来了
            //todo:5.x 不安装插件 少了前两个参数（indexName,type）
            var response= _client.DeleteByQuery<TestModel5>(
                s =>s
                    .Index(indexName)
                    .Query(q =>q.Term(tm => tm.Field(fd => fd.State).Value(1))));
        }
        public void Update(string indexName,int id, TestModel5 model)
        {
            //完整更新
            DocumentPath<TestModel5> path = new DocumentPath<TestModel5>(id);
            var response = _client.Update(path, (p) => p.Index(indexName).Doc(model));
            //或
            //IUpdateRequest<TestModel5, TestModel5> request = new UpdateRequest<TestModel5, TestModel5>(path)
            //{
            //    Doc = new TestModel5()
            //    {
            //        Name = "test4update........"
            //    }
            //};
            //response = _client.Update<TestModel5, TestModel5>(request);

            //局部更新
            //IUpdateRequest<TestModel5, TestModel5P> request1 = new UpdateRequest<TestModel5, TestModel5P>(path)
            //{
            //    Doc = new TestModel5P()
            //    {
            //        Name = "test4update........"
            //    }

            //};
            //response = _client.Update(request1);
            ////或
            //IUpdateRequest<TestModel5, object> request2 = new UpdateRequest<TestModel5, object>(path)
            //{
            //    Doc = new 
            //    {
            //        Name = "test4update........"
            //    }

            //};
            //response = _client.Update(request2);
        }
        public void UpdateByWhere(string indexName)
        {
            //elasticsearch.yml 设置 script.inline: true 
            //目前未开启
            var scriptParams = new Dictionary<string, object> { { "newName", "新的名字" } };
            //todo:5.x 不安装插件 少了前两个参数（indexName,type）
            _client.UpdateByQuery<TestModel5>(
                s =>
                    s.Index(indexName)
                        .Type(typeof (TestModel5))
                        .Query(q => q.Term(t => t.Field(fd => fd.State).Value(1)))
                        .Script(script => script
                                        .Inline("ctx._source.Name = params.newName;")
                                        .Params(scriptParams))
                        );
        }
        public long GetVersion(string indexName, int id)
        {
            DocumentPath<TestModel5> path = new DocumentPath<TestModel5>(id);
            var response = _client.Get<TestModel5>(path, s => s.Index(indexName));
            return response.Version;
        }
        public void VersionLock(string indexName)
        {
            //查询到版本号
            var result = _client.Search<TestModel5>(
                s =>
                    s.Index(indexName)
                        .Query(q => q.Term(tm => tm.Field(fd=>fd.State).Value(1))).Size(1)
                        .Version()//结果中包含版本号
                        );
            foreach (var s in result.Hits)
            {
                Console.WriteLine(s.Id + "  -  " + s.Version);
            }

            var path = new DocumentPath<TestModel5>(1);
            //更新时带上版本号 如果服务端版本号与传入的版本好相同才能更新成功
            var response = _client.Update(path, (p) => p
                .Index(indexName)
                .Type(typeof(TestModel5))
                .Version(2)//限制es中版本号为2时才能成功
                .Doc(new TestModel5() { Name = "测测测" + DateTime.Now })
                );
        }
        public void SearchScanScroll(string indexName)
        {
            Action<int> sc1 = (id) =>
            {
                string scrollid = "";
                //todo:5.x 多了Slice设置 移除SearchType.Scan
                var result = _client.Search<TestModel5>(s => s.Index(indexName).Query(q => q.MatchAll())
                    .Size(15)
                    .Sort(st=>st.Descending(ds=>ds.Id))
                    .Scroll("1m")
                    //id从0开始 0,1,2...
                    //length=max
                    //例：max=3 id=0,id=1,id=2
                    .Slice(sl => sl.Id(id).Max(3))
                    );

                //得到滚动扫描的id
                scrollid = result.ScrollId;
                foreach (var info in result.Documents)
                {
                    Console.WriteLine(info.Id + " - " + " -批次count " + result.Documents.Count + " - 线程"+Thread.CurrentThread.ManagedThreadId);
                }
                while (true)
                {
                    //执行滚动扫描得到数据 返回数据量是 result.Shards.Successful*size(查询成功的分片数*size)
                    var result1 = _client.Scroll<TestModel5>("1m", scrollid);
                    if (result1.Documents == null || !result1.Documents.Any())
                        break;
                    foreach (var info in result1.Documents)
                    {
                        Console.WriteLine(info.Id + " - " +" -批次count "+ result1.Documents.Count+ " - 线程" + Thread.CurrentThread.ManagedThreadId);
                    }
                    //得到新的id
                    scrollid = result1.ScrollId;
                }
            };
           var t1= Task.Factory.StartNew(() => { sc1(0); });
            var t2= Task.Factory.StartNew(() => { sc1(1); });
            var t3= Task.Factory.StartNew(() => { sc1(2); });
            t1.Wait();
            t2.Wait();
            t3.Wait();
        }
        public void Search(string indexName)
        {
            var result = _client.Search<TestModel5>(
                s => s
                    .Index(indexName)//索引
                    .Type(typeof(TestModel5))//类型
                    .Explain() //参数可以提供查询的更多详情。
                    .FielddataFields(fs => fs //对指定字段进行分析
                        .Field(p => p.Name)
                        .Field(p => p.Dic)
                    )
                    .From(0) //跳过的数据个数
                    .Size(50) //返回数据个数
                    .Query(q =>
                        q.Term(p => p.State, 100) // 主要用于精确匹配哪些值，比如数字，日期，布尔值或 not_analyzed的字符串(未经分析的文本数据类型)：
                        &&
                        q.Term(p => p.Name.Suffix("temp"), "姓名") //用于自定义属性的查询 
                        &&
                        q.Bool( //bool 查询
                            b => b
                                //must  should  mushnot
                                .Must(mt => mt //所有分句必须全部匹配，与 AND 相同
                                    .TermRange(p => p.Field(f => f.State).GreaterThan("0").LessThan("1"))) //指定范围查找
                                .Should(sd => sd //至少有一个分句匹配，与 OR 相同
                                    .Term(p => p.State, 32915),
                                    sd => sd.Terms(t => t.Field(fd => fd.State).Terms(new[] { 10, 20, 30 })),
                                    //多值
                                    //||
                                    //sd.Term(p => p.priceID, 1001)
                                    //||
                                    //sd.Term(p => p.priceID, 1005)
                                    sd => sd.TermRange(tr => tr.GreaterThan("10").LessThan("12").Field(f => f.State)),
                                    //出入的时间必须指明时区
                                    sd => sd.DateRange(tr => tr.GreaterThan(DateTime.Now.AddDays(-1)).LessThan(DateTime.Now).Field(f => f.CreateTime))

                                )
                                .MustNot(mn => mn//所有分句都必须不匹配，与 NOT 相同
                                    .Term(p => p.State, 1001)
                                    ,
                                    mn => mn.Bool(
                                        bb => bb.Must(mt => mt
                                              .Match(mc => mc.Field(fd => fd.Name).Query("至尊"))
                                        ))
                                )
                            )
                    )//查询条件
                .Sort(st => st.Ascending(asc => asc.Id))//排序
                //返回特定的字段
                //todo:5.x是sc.Includes
                .Source(sc => sc.Includes(ic => ic
                    .Fields(
                        fd => fd.Name,
                        fd => fd.Id,
                        fd => fd.CreateTime))) 
               );
        }
        public void SearchSetBoost(string indexName)
        {
            // 在原分值基础上 设置不同匹配的加成值 具体算法为lucene内部算法
            var result = _client.Search<TestModel5>(s => s
                            .Index(indexName)
                            .Query(q =>
                                q.Term(t => t
                                            .Field(f => f.State).Value(2).Boost(4))
                                             ||
                                q.Term(t => t
                                            .Field(f => f.State).Value(3).Boost(1))
                                )
                            .Size(3000)
                            .Sort(st => st.Descending(SortSpecialField.Score))
                            );
            //结果中state等于4的得分高
        }
        public void SearchAfter(string indexName)
        {
            var response=_client.Search<TestModel5>(s => s.Index(indexName).Query(q => q.Term(t => t.Field(fd => fd.State).Value(1)))
            .Size(1000)    
            .Sort(st => st.Descending(ds => ds.Id))
                .SearchAfter(new object[]
                {
                    50,//上一次结果排序的最后ID值
                    //可以是多个排序字段的值
                }));
        }
        public void ManyWhereSearch(string indexName)
        {
            bool useStateDesc = true;

            //must 条件
            var mustQuerys = new List<Func<QueryContainerDescriptor<TestModel5>, QueryContainer>>();
            //Deleted
            mustQuerys.Add(mt => mt.Term(tm => tm.Field(fd => fd.Deleted).Value(false)));
            //CreateTime
            mustQuerys.Add(mt => mt.DateRange(tm => tm.Field(fd => fd.CreateTime).GreaterThanOrEquals(DateTime.Now.AddDays(-1)).LessThanOrEquals(DateTime.Now)));
            //should 条件
            var shouldQuerys = new List<Func<QueryContainerDescriptor<TestModel5>, QueryContainer>>();
            //state
            shouldQuerys.Add(mt => mt.Term(tm => tm.Field(fd => fd.State).Value(1)));
            shouldQuerys.Add(mt => mt.Term(tm => tm.Field(fd => fd.State).Value(2)));
            //排序
            Func<SortDescriptor<TestModel5>, IPromise<IList<ISort>>> sortDesc = sd =>
            {
                //根据分值排序
                sd.Descending(SortSpecialField.Score);

                //排序
                if (useStateDesc)
                    sd.Descending(d => d.State);
                else
                    sd.Descending(d => d.Id);
                return sd;
            };
            var result2 =_client.Search<TestModel5>(s => s
                       .Index(indexName)
                       .Query(q => q.Bool(b => b.Must(mustQuerys).Should(shouldQuerys)))
                       .Size(100)
                       .From(0)
                       .Sort(sortDesc)
                   );
        }
        public void FunctionScore(string indexName)
        {
            //使用functionscore计算得分
            var result1 = _client.Search<TestModel5>(s => s
            .Index(indexName)
                            .Query(q => q.FunctionScore(f => f
                                      //查询区
                                      .Query(qq => qq.Term(t => t.Field(fd => fd.State).Value(1))
                                                      ||
                                                   qq.Term(t => t.Field(fd => fd.State).Value(2))
                                      )
                                      .Boost(1.0) //functionscore 对分值影响
                                      .BoostMode(FunctionBoostMode.Replace)//计算boost 模式 ；Replace为替换
                                      .ScoreMode(FunctionScoreMode.Sum) //计算score 模式；Sum为累加
                                                                        //逻辑区
                                      .Functions(fun => fun
                                          .Weight(w => w.Weight(3).Filter(ft => ft
                                              .Term(t => t.Field(fd => fd.State).Value(1))))//匹配cityid +3
                                          .Weight(w => w.Weight(2).Filter(ft => ft
                                              .Term(t => t.Field(fd => fd.State).Value(2))))//匹配pvcid +2
                                      )
                                )
                               )
                            .Size(3000)
                            .Sort(st => st.Descending(SortSpecialField.Score))
                            );
            //结果中 State=1，得分=3； State=2 ，得分=2 ,两者都满足的，得分=5
        }
        public void BaseAggregation(string indexName)
        {
            var result = _client.Search<TestModel5>(s => s
            .Index(indexName)
                .From(0)
                .Size(15)
                .Aggregations(ag => ag
                        .ValueCount("Count", vc => vc.Field(fd => fd.Id))//总数
                        .Sum("vendorPrice_Sum", su => su.Field(fd => fd.Id))//求和
                        .Max("vendorPrice_Max", m => m.Field(fd => fd.Id))//最大值
                        .Min("vendorPrice_Min", m => m.Field(fd => fd.Id))//最小值
                        .Average("vendorPrice_Avg", avg => avg.Field(fd => fd.Id))//平均值
                        .Terms("vendorID_group", t => t.Field(fd => fd.Id).Size(100))//分组
                    )
                );


        }
        public void Aggregations(string indexName)
        {
            var result = _client.Search<TestModel5>(s => s
            .Index(indexName)
                .Size(0)
                .Aggregations(ag => ag
                    .Terms("Group_group", //Group 分组
                        t => t.Field(fd => fd.Group)
                        .Size(100)
                        .Aggregations(agg => agg
                                        .Terms("Group_state_group", //Group_state
                                            tt => tt.Field(fd => fd.State)
                                            .Size(50)
                                            .Aggregations(aggg => aggg
                                                .Average("g_g_Avg", av => av.Field(fd => fd.Dvalue))//Price avg
                                                .Max("g_g_Max", m => m.Field(fd => fd.Dvalue))//Price max
                                                .Min("g_g_Min", m => m.Field(fd => fd.Dvalue))//Price min
                                                .ValueCount("g_g_Count", m => m.Field(fd => fd.Id))//总记录数
                                                )
                                            )
                                        .Cardinality("g_count", dy => dy.Field(fd => fd.State))//分组数量
                                        .ValueCount("g_Count", c => c.Field(fd => fd.Id))
                            )
                        )
                        .Cardinality("vendorID_group_count", dy => dy.Field(fd => fd.Group))//分组数量
                        .ValueCount("Count", c => c.Field(fd => fd.Id))//总记录数
                ) //分组
                );
        }
        public void AnalysisAggregationsResult(string indexName)
        {
            var mustQuerys = new List<Func<QueryContainerDescriptor<TestModel5>, QueryContainer>>();

            mustQuerys.Add(t => t.Term(f => f.Deleted, false));

            var result =
                _client.Search<TestModel5>(
                    s => s.Index(indexName)
                        .Query(q => q
                                .Bool(b => b.Must(mustQuerys))
                        )
                        .Size(0)
                        .Aggregations(ag => ag

                                    .Terms("Group_Group", tm => tm
                                                                .OrderDescending("Dvalue_avg")//使用平均值排序 desc
                                                                .Field(fd => fd.Group)
                                                                .Size(100)
                                                                .Aggregations(agg => agg
                                                                    .TopHits("top_test_hits", th => th.Sort(srt => srt.Field(fd => fd.Dvalue).Descending()).Size(1))//取出该分组下按dvalue分组
                                                                    .Max("Dvalue_Max", m => m.Field(fd => fd.Dvalue))
                                                                    .Min("Dvalue_Min", m => m.Field(fd => fd.Dvalue))
                                                                    .Average("Dvalue_avg", avg => avg.Field(fd => fd.Dvalue))//平均值
                                                                    )

                                                                )


                                )
                        );
            var vendorIdGroup = (BucketAggregate)result.Aggregations["Group_Group"];
            foreach (var bucket1 in vendorIdGroup.Items)
            {
                //todo:2.x KeyedBucket<T> 有泛型参数
                var bucket = (KeyedBucket<object>) bucket1;
                var maxPrice = ((ValueAggregate)bucket.Aggregations["Dvalue_Max"]).Value;
                var minPrice = ((ValueAggregate)bucket.Aggregations["Dvalue_Min"]).Value;
                var sources = ((TopHitsAggregate)bucket.Aggregations["top_test_hits"]).Documents<TestModel5>().ToList();
                var data = sources.FirstOrDefault();
            }
        }
        /// <summary>
        /// null 值查询
        /// 当数据为Null时字段不存在
        /// </summary>
        /// <param name="indexName"></param>
        public void NullValueQuery(string indexName)
        {
            var result = _client.Search<TestModel5>(
               s => s
                   .Index(indexName)//索引
                   .Type(typeof(TestModel5))//类型
                    //fd.Name必须存在  即：fd.Name！=null
                   .Query(q=>q.Bool(b=>b.Must(mt=>mt.Exists(ex=>ex.Field(fd=>fd.Name))))
                   ));
            
        }
        /// <summary>
        /// 空字符查询
        /// </summary>
        /// <param name="indexName"></param>
        public void StringEmptyQuery(string indexName)
        {
            var result = _client.Search<TestModel5>(
              s => s
                  .Index(indexName)//索引
                  .Type(typeof(TestModel5))//类型
                   //.Verbatim()如果不加该查询条件无效
                  .Query(q => q.Bool(b => b.Must(mt => mt.Term(ex => ex.Verbatim().Field(fd => fd.Name).Value(""))))
                  ));
        }
    }
}
