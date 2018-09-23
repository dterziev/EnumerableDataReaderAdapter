using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace EnumerableDataReaderAdapter
{
    using static Expression;

    public class ColumnMappings<T>
    {
        private static readonly (string ColumnName, Type ColumnType, Func<T, object> ValueGetter)[] _defaultMappings;

        static ColumnMappings()
        {
            _defaultMappings = typeof(T)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetProperty)
                .Select(p =>
                {
                    string columnName = p.Name;
                    var parameterExpression = Parameter(typeof(T), "p");

                    var expr = 
                        Lambda(
                            Convert(
                                Property(parameterExpression, columnName), 
                                typeof(object)),
                            parameterExpression
                        );

                    var accessor = (Func<T, object>)expr.Compile();

                    return (ColumnName: columnName, ColumnType: p.PropertyType, ValueGetter: accessor);
                }).ToArray();
        }

        private List<(string ColumnName, Type ColumnType, Func<T, object> ValueGetter)> _mappings = new List<(string ColumnName, Type ColumnType, Func<T, object> ValueGetter)>();

        public ColumnMappings<T> Add(string columnName, Type type, Func<T, object> valueGetter)
        {
            if (string.IsNullOrWhiteSpace(columnName))
            {
                throw new ArgumentException("Column name cannot be null or empty string.", nameof(columnName));
            }

            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            if (valueGetter == null)
            {
                throw new ArgumentNullException(nameof(valueGetter));
            }

            if (_mappings.Any(m => m.ColumnName == columnName))
                throw new ArgumentException($"Column name {columnName} is already mapped.", nameof(columnName));

            _mappings.Add((columnName, type, valueGetter));
            return this;
        }

        public ColumnMappings<T> Add(Expression<Func<T, object>> propertyExpression)
        {
            if (propertyExpression == null)
            {
                throw new ArgumentNullException(nameof(propertyExpression));
            }

            var body = (propertyExpression as LambdaExpression).Body;
            MemberExpression m = null;
            if (body is UnaryExpression ue) m = ue.Operand as MemberExpression;
            else m = body as MemberExpression;

            if (m == null) throw new ArgumentException("Expected a member expression.", nameof(propertyExpression));

            _mappings.Add(
                (ColumnName: m.Member.Name,
                ColumnType: m.Type, 
                ValueGetter: propertyExpression.Compile()));

            return this;
        }

        internal (string ColumnName, Type ColumnType, Func<T, object> ValueGetter)[] GetMappings()
        {
            if (_mappings != null && _mappings.Count > 0) return _mappings.ToArray();
            else return _defaultMappings;
        }
    }
}
