using System.Data;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace EnumerableDataReaderAdapter.Benchmarks
{
    internal class DataStructure
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

    }

    [SimpleJob(BenchmarkDotNet.Jobs.RuntimeMoniker.Net60)]
    [SimpleJob(BenchmarkDotNet.Jobs.RuntimeMoniker.Net70)]
    [SimpleJob(BenchmarkDotNet.Jobs.RuntimeMoniker.Net80)]
    [RPlotExporter, RankColumn]
    [MemoryDiagnoser]
    public class EnumerableDataReaderBenchmark
    {
        private DataStructure[] _data = default!;
        private object[] _buffer = default!;

        static readonly ColumnMappings<DataStructure> _mappingDelegates = new ColumnMappings<DataStructure>()
            .Add(nameof(DataStructure.StringField), typeof(string), p => p.StringField)
            .Add(nameof(DataStructure.IntField), typeof(int), p => p.IntField)
            .Add(nameof(DataStructure.NullableIntField), typeof(int?), p => p.NullableIntField);

        static readonly ColumnMappings<DataStructure> _mappingExpressions = new ColumnMappings<DataStructure>()
            .Add(p => p.StringField)
            .Add(p => p.IntField)
            .Add(p => p.NullableIntField);

        [Params(10000)]
        public int N;

        [GlobalSetup]
        public void Setup()
        {
            _data = Enumerable.Range(1, N)
                .Select(i => new DataStructure($"{i}", i, (i % 2) == 0 ? (int?)null : i))
                .ToArray();
            _buffer = new object[3];
        }

        [Benchmark(Baseline = true)]
        public void DefaultMapping_ByColumnIndex()
        {
            using (var reader = _data.ToDataReader())
            {
                while (reader.Read()) GetDataByColumnIndex(reader);
            }
        }

        [Benchmark]
        public void MappingExpressions_ByColumnIndex()
        {
            using (var reader = _data.ToDataReader(
                mapping => mapping
                    .Add(p => p.StringField)
                    .Add(p => p.IntField)
                    .Add(p => p.NullableIntField)
            ))
            {
                while (reader.Read()) GetDataByColumnIndex(reader);
            }
        }

        [Benchmark]
        public void MappingExpressions_ByColumnIndex_CachedMapping()
        {
            using (var reader = _data.ToDataReader(_mappingExpressions))
            {
                while (reader.Read()) GetDataByColumnIndex(reader);
            }
        }

        [Benchmark]
        public void MappingDelegates_ByColumnIndex()
        {
            using (var reader = _data.ToDataReader(
                mapping => mapping
                    .Add(nameof(DataStructure.StringField), typeof(string), p => p.StringField)
                    .Add(nameof(DataStructure.IntField), typeof(int), p => p.IntField)
                    .Add(nameof(DataStructure.NullableIntField), typeof(int?), p => p.NullableIntField)
            ))
            {
                while (reader.Read()) GetDataByColumnIndex(reader);
            }
        }

        [Benchmark]
        public void MappingDelegates_ByColumnIndex_CachedMapping()
        {
            using (var reader = _data.ToDataReader(_mappingDelegates))
            {
                while (reader.Read()) GetDataByColumnIndex(reader);
            }
        }

        [Benchmark()]
        public void DefaultMapping_ByColumnName()
        {
            using (var reader = _data.ToDataReader())
            {
                while (reader.Read()) GetDataByColumnName(reader);
            }
        }

        [Benchmark]
        public void MappingExpressions_ByColumnName()
        {
            using (var reader = _data.ToDataReader(
                mapping => mapping
                    .Add(p => p.StringField)
                    .Add(p => p.IntField)
                    .Add(p => p.NullableIntField)
            ))
            {
                while (reader.Read()) GetDataByColumnName(reader);
            }
        }

        [Benchmark]
        public void MappingExpressions_ByColumnName_CachedMapping()
        {
            using (var reader = _data.ToDataReader(_mappingExpressions)) 
            {
                while (reader.Read()) GetDataByColumnName(reader);
            }
        }


        [Benchmark]
        public void MappingDelegates_ByColumnName()
        {
            using (var reader = _data.ToDataReader(
                mapping => mapping
                    .Add(nameof(DataStructure.StringField), typeof(string), p => p.StringField)
                    .Add(nameof(DataStructure.IntField), typeof(int), p => p.IntField)
                    .Add(nameof(DataStructure.NullableIntField), typeof(int?), p => p.NullableIntField)
            ))
            {
                while (reader.Read()) GetDataByColumnName(reader);
            }
        }

        [Benchmark]
        public void MappingDelegates_ByColumnName_CachedMapping()
        {
            using (var reader = _data.ToDataReader(_mappingDelegates)) 
            {
                while (reader.Read()) GetDataByColumnName(reader);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void GetDataByColumnIndex(IDataReader reader)
        {
            _buffer[0] = reader[0];
            _buffer[1] = reader[1];
            _buffer[2] = reader[2];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void GetDataByColumnName(IDataReader reader)
        {
            _buffer[0] = reader[nameof(DataStructure.StringField)];
            _buffer[1] = reader[nameof(DataStructure.IntField)];
            _buffer[2] = reader[nameof(DataStructure.NullableIntField)];
        }

    }

    public class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
