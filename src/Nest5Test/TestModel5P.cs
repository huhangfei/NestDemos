using Nest;

namespace Nest5Test
{
    /// <summary>
    /// 5.x 特性
    /// </summary>
    [ElasticsearchType(Name = "TestModel", IdProperty = "Id")]
    public class TestModel5P
    {
        [Number(NumberType.Long, Name = "Id")]
        public long Id { get; set; }

        /// <summary>
        /// keyword 不分词
        /// </summary>
        [Keyword(Name = "Name", Index = true)]
        public string Name { get; set; }

        /// <summary>
        /// text 分词,Analyzer = "ik_max_word"
        /// </summary>
        [Text(Name = "Dic", Index = true)]
        public string Dic { get; set; }

        [Number(NumberType.Integer, Name = "State")]
        public int State { get; set; }
    }
}