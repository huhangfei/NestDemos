using System;
using System.Collections.Generic;
using System.Configuration;
using Elasticsearch.Net;
using Nest;

namespace EsData.Utils
{
    /// <summary>
    /// eshelper
    /// </summary>
    public class EsHelper
    {
        private static Dictionary<string, IElasticClient> clients=new Dictionary<string, IElasticClient>();
        private static object myLock = new object();
        private static string defaultNodesConfigKey = "Nodes";


        /// <summary>
        /// 根据配置得到集合
        /// </summary>
        /// <returns></returns>
        private static List<Uri> GetAllNodes(string nodesConfigKey= null)
        {
            nodesConfigKey = nodesConfigKey == null ? defaultNodesConfigKey : nodesConfigKey;

            string nodes = ConfigurationManager.AppSettings[nodesConfigKey] == null ? string.Empty : ConfigurationManager.AppSettings[nodesConfigKey].Trim();
            
            return NodesParse(nodes);
        }
        /// <summary>
        /// 解析nodes
        /// </summary>
        /// <param name="nodes"></param>
        /// <returns></returns>
        private static List<Uri> NodesParse(string nodes)
        {
            if (string.IsNullOrEmpty(nodes))
                return null;
            nodes = nodes.Trim();
            string[] nodeshost = nodes.Split(';');
            var nodUris = new List<Uri>();
            for (int i = 0; i < nodeshost.Length; i++)
            {
                if (!string.IsNullOrEmpty(nodeshost[i]))
                    nodUris.Add(new Uri(string.Format("http://{0}", nodeshost[i])));
            }
            return nodUris;
        }
        /// <summary>
        /// 创建新的es client
        /// </summary>
        /// <param name="nodes"></param>
        /// <param name="indexName"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private static IElasticClient CreateElasticClient(List<Uri> nodes,string indexName, int timeout)
        {
            //var node = new Uri("http://192.168.87.13:9200");
            //var node = new Uri("http://localhost:9200");
            //基本设置
            //var settings = new ConnectionSettings(node).DefaultIndex(_indexName);
            //指定某种类型对应某个索引
            //var settings = new ConnectionSettings(node).MapDefaultTypeIndices(m => m.Add(typeof(MyClass),"test-2").Add(typeof(VendorPriceInfo),"test-3"));

            //节点
            if(nodes==null)
                nodes = GetAllNodes();

            if (nodes == null || nodes.Count == 0)
                throw new Exception("未配置ES节点!(Nodes)");

            //链接池
            //对单节点请求
            //IConnectionPool pool = new SingleNodeConnectionPool(node);
            //请求时随机请求各个正常节点，不请求异常节点,异常节点恢复后会重新被请求
            IConnectionPool pool = new StaticConnectionPool(nodes);

            //IConnectionPool pool = new SniffingConnectionPool(urls);
            //false.创建客户端时，随机选择一个节点作为客户端的请求对象，该节点异常后不会切换其它节点
            //true，请求时随机请求各个正常节点，不请求异常节点,但异常节点恢复后不会重新被请求
            //pool.SniffedOnStartup = true;

            //IConnectionPool pool = new StickyConnectionPool(urls);
            //创建客户端时，选择第一个节点作为请求主节点，该节点异常后会切换其它节点，待主节点恢复后会自动切换回来

            var settings = new ConnectionSettings(pool);
            if (!string.IsNullOrEmpty(indexName))
                settings.DefaultIndex(indexName);
            settings.RequestTimeout(new TimeSpan(timeout * 10000));
            //settings.DisableDirectStreaming();
            settings.DefaultFieldNameInferrer(name => name);
            settings.ThrowExceptions();
            return new ElasticClient(settings);


        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="nodesConfigKey"></param>
        /// <param name="indexName"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private static IElasticClient CreateElasticClient(string nodesConfigKey, string indexName, int timeout)
        {
            return CreateElasticClient(GetAllNodes(nodesConfigKey), indexName, timeout);
        }
        /// <summary>
        /// 获取 es client
        /// </summary>
        /// <param name="nodes"></param>
        /// <param name="indexName"></param>
        /// <param name="timeout">请求超时时间（毫秒）</param>
        /// <param name="single"></param>
        /// <returns></returns>
        public static IElasticClient CreateElasticClient(
            string nodesConfigKey = null,
            string indexName = "",
            int timeout = 3000,
            bool single=true)
        {
            if (!single)
                return CreateElasticClient(nodesConfigKey, indexName, timeout);

            if (clients.ContainsKey(nodesConfigKey))
            {
                return clients[nodesConfigKey];
            }

            lock (myLock) {
                if (!clients.ContainsKey(nodesConfigKey))
                {
                    lock (myLock) {
                        clients.Add(nodesConfigKey,CreateElasticClient(nodesConfigKey, indexName, timeout));
                    }
                }
            }
            return clients[nodesConfigKey];
        }
       
    }
}
