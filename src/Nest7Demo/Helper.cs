using System;
using System.Collections.Generic;
using System.Configuration;

namespace Nest7Demo
{
    /// <summary>
    /// es client Factory
    /// </summary>
    public class Helper
    {
        private static readonly string nodeConfigKey = "Nodes";
        /// <summary>
        /// 根据配置得到集合
        /// </summary>
        /// <returns></returns>
        public static List<Uri> GetAllNodes()
        {
            string nodes = ConfigurationManager.AppSettings[nodeConfigKey] == null ? string.Empty : ConfigurationManager.AppSettings[nodeConfigKey].Trim();
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
    }
}
