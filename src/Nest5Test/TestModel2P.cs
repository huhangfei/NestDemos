using Nest;

namespace Nest5Test
{
    /// <summary>
    /// 5.x 特性
    /// </summary>
    [ElasticsearchType(Name = "TestModel", IdProperty = "Id")]
    public class TestModel2P
    {
        [Number(NumberType.Long, Name = "Id")]
        public long Id { get; set; }

        /// <summary>
        /// keyword 不分词
        /// </summary>
        [String(Name = "Name", Index = FieldIndexOption.NotAnalyzed)]
        public string Name { get; set; }

        /// <summary>
        /// text 分词,Analyzer = "ik_max_word"
        /// </summary>
        [String(Name = "Dic", Index = FieldIndexOption.Analyzed)]
        public string Dic { get; set; }

        [Number(NumberType.Integer, Name = "State")]
        public int State { get; set; }
    }
}