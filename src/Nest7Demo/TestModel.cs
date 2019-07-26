using System;
using Nest;

namespace Nest7Demo
{
    /// <summary>
    ///  
    /// </summary>
    [ElasticsearchType(RelationName = "TestModel", IdProperty = "Id")]
    public class TestModel
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

        [Number(NumberType.Integer, Name = "Group")]
        public int Group { get; set; }

        [Boolean(Name = "Deleted")]
        public bool Deleted { get; set; }

        [Date(Name = "AddTime")]
        public DateTime AddTime { get; set; }

        [Number(NumberType.Float, Name = "PassingRate")]
        public float PassingRate { get; set; }

        [Number(NumberType.Double, Name = "Dvalue")]
        public double Dvalue { get; set; }

        private DateTime _createTime;

        [Date(Name = "CreateTime")]
        public DateTime CreateTime
        {
            get { return _createTime.ToLocalTime(); }
            set
            {
                //由于从数据库查出的时间 Kind = Unspecified 所以主动设置为 本地
                //否则 写入es时会把未知默认作为 UTC，实则是本地时间
                //通过本字段查询的时候赋值时也需注意 传入的时间对象Kind值不能为 Unspecified
                if (value.Kind == DateTimeKind.Unspecified)
                    value = DateTime.SpecifyKind(value, DateTimeKind.Local);
                _createTime = value;
            }
        }
    }
}