using System.Collections;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace EnumerableDataReaderAdapter
{
    public static class EnumerableExtensions
    {
        public static IDataReader ToDataReader<T>(
            this IEnumerable<T> data,
            Action<ColumnMappings<T>>? configureMappings = null)
        {
            var columnMappings = new ColumnMappings<T>();
            configureMappings?.Invoke(columnMappings);
            return ToDataReader(data, columnMappings);
        }

        public static IDataReader ToDataReader<T>(
            this IEnumerable<T> data,
            ColumnMappings<T> columnMappings)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (columnMappings == null)
            {
                throw new ArgumentNullException(nameof(columnMappings));
            }

            return new EnumerableReaderAdapter<T>(data, columnMappings.GetMappings());
        }

        private sealed class EnumerableReaderAdapter<T> : DbDataReader
        {
            private readonly (string ColumnName, Type ColumnType, Func<T, object?> ValueGetter)[] _mappings;
            private bool _isClosed = false;
            private IEnumerator<T> _enumerator;
            private T _current = default!;
            private readonly Lazy<Dictionary<string, int>> _columnLookup;
            private long _rowCount = 0;

            public EnumerableReaderAdapter(
                IEnumerable<T> rows,
                (string ColumnName, Type ColumnType, Func<T, object?> ValueGetter)[] mappings)
            {
                _enumerator = rows.GetEnumerator();
                _mappings = mappings;
                _columnLookup = new Lazy<Dictionary<string, int>>(() =>
                {
                    var result = new Dictionary<string, int>(_mappings.Length);
                    for (int i = 0; i < _mappings.Length; i++)
                    {
                        result.Add(_mappings[i].ColumnName, i);
                    }
                    return result;
                });
            }

            public override bool HasRows => true;

            public override IEnumerator GetEnumerator()
            {
                return new DbEnumerator(this, true);
            }

            public override DataTable GetSchemaTable()
            {
                DataTable table = new("SchemaTable");
                table.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
                table.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
                table.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
                table.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(Int16));
                table.Columns.Add(SchemaTableColumn.NumericScale, typeof(Int16));
                table.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
                table.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
                table.Columns.Add(SchemaTableColumn.IsKey, typeof(bool));

                table.BeginLoadData();
                for (int i = 0; i < _mappings.Length; i++)
                {
                    DataRow row = table.NewRow();
                    row[SchemaTableColumn.ColumnName] = _mappings[i].ColumnName;
                    row[SchemaTableColumn.ColumnOrdinal] = i;
                    row[SchemaTableColumn.ColumnSize] = -1;
                    row[SchemaTableColumn.DataType] = _mappings[i].ColumnType;
                    row[SchemaTableColumn.AllowDBNull] = false;
                    row[SchemaTableColumn.IsKey] = false;
                    table.Rows.Add(row);
                }

                table.EndLoadData();

                return table;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override bool Read()
            {
                bool next = _enumerator.MoveNext();
                if (next)
                {
                    _rowCount += 1;
                    _current = _enumerator.Current;
                }
                else
                {
                    _current = default!;
                }

                return next;
            }

            public override void Close()
            {
                if (!_isClosed)
                {
                    _isClosed = true;
                    _enumerator.Dispose();
                    _enumerator = null!;
                    _current = default!;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override int GetOrdinal(string name) => _columnLookup.Value[name];

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override object GetValue(int i) => _mappings[i].ValueGetter(_current)!;

            public override int GetValues(object?[] values)
            {
                var max = values.Length < _mappings.Length
                    ? values.Length
                    : _mappings.Length;

                for (int i = 0; i < max; i++)
                {
                    values[i] = _mappings[i].ValueGetter(_current);
                }

                return max;
            }

            public override object this[int i] => GetValue(i);
            public override object this[string name] => GetValue(GetOrdinal(name));
            public override bool NextResult() => false;
            public override int Depth => 0;
            public override bool IsClosed => _isClosed;
            public override int RecordsAffected => (int)_rowCount;
            public override string GetName(int i) => _mappings[i].ColumnName;
            public override string GetDataTypeName(int i) => null!;
            public override Type GetFieldType(int i) => null!;
            public override bool GetBoolean(int i) => (bool)GetValue(i);
            public override byte GetByte(int i) => (byte)GetValue(i);
            public override long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
            public override char GetChar(int i) => (char)GetValue(i);
            public override long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
            public override Guid GetGuid(int i) => (Guid)GetValue(i);
            public override short GetInt16(int i) => (short)GetValue(i);
            public override int GetInt32(int i) => (int)GetValue(i);
            public override long GetInt64(int i) => (long)GetValue(i);
            public override float GetFloat(int i) => (float)GetValue(i);
            public override double GetDouble(int i) => (double)GetValue(i);
            public override string GetString(int i) => (string)GetValue(i);
            public override decimal GetDecimal(int i) => (decimal)GetValue(i);
            public override DateTime GetDateTime(int i) => (DateTime)GetValue(i);
            public override bool IsDBNull(int i) => GetValue(i) == null || GetValue(i) == DBNull.Value;
            public override int FieldCount => _mappings.Length;
        }
    }
}
