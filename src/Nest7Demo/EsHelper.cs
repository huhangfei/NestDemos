using System;
using System.Collections.Generic;
using System.Configuration;
using Elasticsearch.Net;
using Nest;

namespace Nest7Demo
{
    /// <summary>
    /// eshelper
    /// </summary>
    public class EsHelper
    {
        private static Dictionary<string, IElasticClient> clients = new Dictionary<string, IElasticClient>();
        private static object myLock = new object();
        private const string defaultNodesConfigKey = "EsConnectionString";
        Dictionary<string, string> connectionConfigs = new Dictionary<string, string>();
        /// <summary>
        /// 根据配置得到集合
        /// </summary>
        /// <returns></returns>
        private static List<Uri> GetAllNodes(string nodesConfigKey)
        {
            Dictionary<string, string> keyValues = GetConnectionConfigs(nodesConfigKey);
            if (keyValues.ContainsKey("hosts"))
            {
                return NodesParse(keyValues["hosts"]);
            }
            return null;
        }
        private static string GetUserName(string nodesConfigKey)
        {
            Dictionary<string, string> keyValues = GetConnectionConfigs(nodesConfigKey);
            if (keyValues.ContainsKey("user"))
            {
                return keyValues["user"];
            }
            return null;
        }
        private static string GetPassword(string nodesConfigKey)
        {
            Dictionary<string, string> keyValues = GetConnectionConfigs(nodesConfigKey);
            if (keyValues.ContainsKey("password"))
            {
                return keyValues["password"];
            }
            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="nodesConfigKey"></param>
        /// <returns></returns>
        private static Dictionary<string, string> GetConnectionConfigs(string nodesConfigKey)
        {
            string connectionString = ConfigurationManager.AppSettings[nodesConfigKey] == null ? string.Empty : ConfigurationManager.AppSettings[nodesConfigKey].Trim();
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new Exception($"未找到es配置{nodesConfigKey}");
            }
            string[] connectionConfigArr = connectionString.Split(';');
            Dictionary<string, string> keyValues = new Dictionary<string, string>(connectionConfigArr.Length);

            for (int i = 0; i < connectionConfigArr.Length; i++)
            {
                if (!string.IsNullOrEmpty(connectionConfigArr[i]))
                {
                    string[] kv = connectionConfigArr[i].Split('=');
                    keyValues.Add(kv[0], kv[1]);
                }
            }

            return keyValues;
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
        private static IElasticClient CreateElasticClient(List<Uri> nodes, string indexName, int timeout,string user=null,string password = null)
        {
            if (nodes == null || nodes.Count == 0)
                throw new Exception("未配置ES节点!(Nodes)");
            //var node = new Uri("http://192.168.87.13:9200");
            //var node = new Uri("http://localhost:9200");
            //基本设置
            //var settings = new ConnectionSettings(node).DefaultIndex(_indexName);
            //指定某种类型对应某个索引
            //var settings = new ConnectionSettings(node).MapDefaultTypeIndices(m => m.Add(typeof(MyClass),"test-2").Add(typeof(VendorPriceInfo),"test-3"));

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
            settings.DisableDirectStreaming();
            settings.DefaultFieldNameInferrer(name => name);
            //settings.ThrowExceptions();


            if (!string.IsNullOrEmpty(user) && !string.IsNullOrEmpty(password))
            {
                settings.BasicAuthentication(user, password);
            }

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
            string user = GetUserName(nodesConfigKey);
            string password = GetPassword(nodesConfigKey);
            List<Uri> nodes = GetAllNodes(nodesConfigKey);
            return CreateElasticClient(nodes, indexName, timeout, user,password);
        }
        /// <summary>
        /// 获取 es client
        /// </summary>
        /// <param name="nodesConfigKey">nodes配置 所在appSettings项的key</param>
        /// <param name="indexName">默认的索引名</param>
        /// <param name="timeout">请求超时时间（毫秒）</param>
        /// <param name="single">是否创建单例对象</param>
        /// <returns></returns>
        public static IElasticClient CreateElasticClient(string nodesConfigKey = null, string indexName = "", int timeout = 3000, bool single = true)
        {
            //配置key缺省时，使用默认key值
            nodesConfigKey = string.IsNullOrEmpty(nodesConfigKey) ? defaultNodesConfigKey : nodesConfigKey;

            if (!single)
                return CreateElasticClient(nodesConfigKey, indexName, timeout);

            string connectionKey = nodesConfigKey + ":" + indexName;

            if (clients.ContainsKey(connectionKey))
            {
                return clients[connectionKey];
            }
            lock (myLock)
            {
                if (!clients.ContainsKey(connectionKey))
                {
                    lock (myLock)
                    {
                        clients.Add(connectionKey, CreateElasticClient(nodesConfigKey, indexName, timeout));
                        
                    }
                }
            }
            return clients[connectionKey];
        }
    }
}
