using System;
using System.Data;
using System.Linq;
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

    [SimpleJob(BenchmarkDotNet.Jobs.RuntimeMoniker.Net472)]
    [SimpleJob(BenchmarkDotNet.Jobs.RuntimeMoniker.NetCoreApp31)]
    [SimpleJob(BenchmarkDotNet.Jobs.RuntimeMoniker.Net60)]
    [RPlotExporter, RankColumn]
    [MemoryDiagnoser]
    public class EnumerableDataReaderBenchmark
    {
        private DataStructure[] data;
        private object[] buffer;

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
            data = Enumerable.Range(1, N)
                .Select(i => new DataStructure($"{i}", i, (i % 2) == 0 ? (int?)null : i))
                .ToArray();
            buffer = new object[3];
        }

        [Benchmark(Baseline = true)]
        public void DefaultMapping_ByColumnIndex()
        {
            using (var reader = data.ToDataReader())
            {
                while (reader.Read()) GetDataByColumnIndex(reader);
            }
        }

        [Benchmark]
        public void MappingExpressions_ByColumnIndex()
        {
            using (var reader = data.ToDataReader(
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
            using (var reader = data.ToDataReader(_mappingExpressions))
            {
                while (reader.Read()) GetDataByColumnIndex(reader);
            }
        }

        [Benchmark]
        public void MappingDelegates_ByColumnIndex()
        {
            using (var reader = data.ToDataReader(
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
            using (var reader = data.ToDataReader(_mappingDelegates))
            {
                while (reader.Read()) GetDataByColumnIndex(reader);
            }
        }

        [Benchmark()]
        public void DefaultMapping_ByColumnName()
        {
            using (var reader = data.ToDataReader())
            {
                while (reader.Read()) GetDataByColumnName(reader);
            }
        }

        [Benchmark]
        public void MappingExpressions_ByColumnName()
        {
            using (var reader = data.ToDataReader(
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
            using (var reader = data.ToDataReader(_mappingExpressions)) 
            {
                while (reader.Read()) GetDataByColumnName(reader);
            }
        }


        [Benchmark]
        public void MappingDelegates_ByColumnName()
        {
            using (var reader = data.ToDataReader(
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
            using (var reader = data.ToDataReader(_mappingDelegates)) 
            {
                while (reader.Read()) GetDataByColumnName(reader);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void GetDataByColumnIndex(IDataReader reader)
        {
            buffer[0] = reader[0];
            buffer[1] = reader[1];
            buffer[2] = reader[2];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void GetDataByColumnName(IDataReader reader)
        {
            buffer[0] = reader[nameof(DataStructure.StringField)];
            buffer[1] = reader[nameof(DataStructure.IntField)];
            buffer[2] = reader[nameof(DataStructure.NullableIntField)];
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
