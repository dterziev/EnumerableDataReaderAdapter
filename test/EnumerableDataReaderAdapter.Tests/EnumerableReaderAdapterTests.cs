using System.Data;

namespace EnumerableDataReaderAdapter.Tests
{
    public class EnumerableReaderAdapterTests
    {
        private class DataStructure
        {
            public DataStructure(
                string stringField, 
                int intField, 
                int? nullableIntField)
            {
                StringField = stringField ?? throw new ArgumentNullException(nameof(stringField));
                IntField = intField;
                NullableIntField = nullableIntField;
            }

            public string StringField { get; }
            public int IntField { get; }
            public int? NullableIntField { get; }

            public static string[] ColumnNames => typeof(DataStructure).GetProperties().Select(p => p.Name).ToArray();
            public static Type[] ColumnTypes => typeof(DataStructure).GetProperties().Select(p => p.PropertyType).ToArray();
        }


        [Fact]
        public void Read_WhenEmpty_ReturnsFalse()
        {
            using (IDataReader reader = Enumerable.Empty<DataStructure>().ToDataReader())
            {
                Assert.False(reader.Read());
            }
        }

        [Fact]
        public void Read_WithDefaultMapping_ProducesCorrectResults()
        {
            IEnumerable<DataStructure> data = new[] {
                new DataStructure("abcd", 1, null),
                new DataStructure("abcd", 1, 4),
            };

            IDataReader reader = data.ToDataReader();

            using (reader)
            {
                int i = 0;
                while (reader.Read())
                {
                    Assert.Equal("abcd", reader[nameof(DataStructure.StringField)]);
                    Assert.Equal(1, reader[nameof(DataStructure.IntField)]);
                    var expected = i == 0 ? null : (int?)4;
                    Assert.Equal(expected, reader[nameof(DataStructure.NullableIntField)]);
                    i++;
                }
            }
        }

        [Fact]
        public void Read_WithCustomMapping_ProducesCorrectResults()
        {
            IEnumerable<DataStructure> data = new[] {
                new DataStructure("abcd", 1, null),
                new DataStructure("abcd", 1, 4),
            };

            IDataReader reader = data.ToDataReader(
                m => m
                .Add(d => d.StringField)
                .Add("IntField", typeof(int), d => d.IntField)
                .Add(d => d.NullableIntField)
                .Add("Const", typeof(int), d => -1));

            using (reader)
            {
                int i = 0;
                while (reader.Read())
                {
                    Assert.Equal("abcd", reader.GetValue(0));
                    Assert.Equal(1, reader.GetValue(1));
                    var expected = i == 0 ? null : (int?)4;
                    Assert.Equal(expected, reader.GetValue(2));
                    Assert.Equal(-1, reader["Const"]);
                    i++;
                }
            }
        }

        [Fact]
        public void ToDataReader_WithIncorrectMapping_Throws()
        {
            IEnumerable<DataStructure> data = new DataStructure[0];

            Assert.Throws<ArgumentException>(() =>
            {
                using (IDataReader reader = data.ToDataReader(m => m.Add(d => -1)))
                {
                }
            });
        }

        [Fact]
        public void ToDataReader_WithDuplicateColumnNames_Throws()
        {
            IEnumerable<DataStructure> data = new DataStructure[0];

            Assert.Throws<ArgumentException>(() =>
            {
                using (IDataReader reader = data.ToDataReader(m => m.Add("A", typeof(object), d => -1).Add("A", typeof(object), d => -1)))
                {
                }
            });
        }
    }
}
