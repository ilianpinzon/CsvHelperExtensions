using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace CsvHelperExtensions
{
    public static class DynamicRecordExtensions
    {
        /// <summary>
        /// Drops the given <paramref name="column"/>.
        /// </summary>
        public static IEnumerable<dynamic> DropColumn(this IEnumerable<dynamic> records, string column)
        {
            if (records == null) throw new ArgumentNullException(nameof(records));
            if (column == null) throw new ArgumentNullException(nameof(column));

            return ForEach(records,
                (in TxArg item) =>
                {
                    if (item.PropName != column)
                        item.OutObjDict.Add(item.PropName, item.PropValue);
                });
        }

        /// <summary>
        /// Drops columns as specified by <paramref name="startIndex"/> and <paramref name="count"/>.
        /// </summary>
        public static IEnumerable<dynamic> DropColumns(this IEnumerable<dynamic> records, int startIndex, int count)
        {
            if (records == null) throw new ArgumentNullException(nameof(records));
            if (startIndex <= 0) throw new ArgumentOutOfRangeException(nameof(startIndex));

            return ForEach(records,
                (in TxArg item) =>
                {
                    int endIndex = startIndex + count;
                    if (item.ColumnIndex < startIndex || item.ColumnIndex >= endIndex)
                        item.OutObjDict.Add(item.PropName, item.PropValue);
                });
        }

        /// <summary>
        /// Fills a <paramref name="column"/> with <paramref name="newValue"/> if <paramref name="predicate"/> is true.
        /// </summary>
        public static IEnumerable<dynamic> FillColumn(this IEnumerable<dynamic> records, string column,
            string newValue, Func<string, bool> predicate)
        {
            if (records == null) throw new ArgumentNullException(nameof(records));
            if (column == null) throw new ArgumentNullException(nameof(column));
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            return ForEach(records,
                (in TxArg item) =>
                {
                    if (item.PropName == column && predicate(item.PropValue))
                        item.OutObjDict.Add(item.PropName, newValue);
                    else
                        item.OutObjDict.Add(item.PropName, item.PropValue);
                });
        }

        /// <summary>
        /// Inserts a new column at <paramref name="columnIndex"/> with the given header (<paramref name="column"/>)
        /// with the value returned by <paramref name="selector"/>.
        /// </summary>
        public static IEnumerable<dynamic> InsertColumn(this IEnumerable<dynamic> records, int columnIndex,
            string column, Func<dynamic, string> selector)
        {
            if (records == null) throw new ArgumentNullException(nameof(records));
            if (column == null) throw new ArgumentNullException(nameof(column));
            if (selector == null) throw new ArgumentNullException(nameof(selector));
            if (columnIndex < 0) throw new ArgumentOutOfRangeException(nameof(columnIndex));

            return ForEach(records,
                (in TxArg item) =>
                {
                    if (item.ColumnIndex == columnIndex)
                        item.OutObjDict.Add(column, selector(item.OrigObjDict));

                    item.OutObjDict.Add(item.PropName, item.PropValue);

                    // Allow adding at the end
                    int nextIndex = item.ColumnIndex + 1;
                    if (nextIndex == item.OrigObjDict.Keys.Count && nextIndex == columnIndex)
                        item.OutObjDict.Add(column, selector(item.OrigObjDict));
                });
        }

        /// <summary>
        /// Replaces the column headers of <paramref name="columns"/>.
        /// </summary>
        public static IEnumerable<dynamic> ReplaceColumnHeaders(this IEnumerable<dynamic> records,
            params string[] columns)
        {
            return ReplaceColumnHeaders(records, (IReadOnlyList<string>)columns);
        }

        /// <summary>
        /// Replaces the column headers of <paramref name="columns"/>.
        /// </summary>
        public static IEnumerable<dynamic> ReplaceColumnHeaders(this IEnumerable<dynamic> records,
            IReadOnlyList<string> columns)
        {
            if (records == null) throw new ArgumentNullException(nameof(records));
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            return ForEach(records,
                (in TxArg item) =>
                {
                    if (item.ColumnIndex < columns.Count)
                        item.OutObjDict.Add(columns[item.ColumnIndex], item.PropValue);

                    // If we have more new column headers, insert new columns with empty strings
                    if (item.ColumnIndex == item.OrigObjDict.Keys.Count - 1)
                    {
                        for (int i = item.OrigObjDict.Keys.Count; i < columns.Count; ++i)
                        {
                            item.OutObjDict.Add(columns[i], string.Empty);
                        }
                    }
                });
        }

        private static IEnumerable<dynamic> ForEach(IEnumerable<dynamic> records, ActionIn<TxArg> txAction)
        {
            return records.Cast<IDictionary<string, object>>()
                .Select(origObjDict =>
                {
                    IDictionary<string, object> outObjDict = new ExpandoObject();

                    int columnIndex = 0;
                    foreach (var pair in origObjDict)
                    {
                        var transformArg = new TxArg
                        {
                            OrigObjDict = origObjDict,
                            OutObjDict = outObjDict,
                            ColumnIndex = columnIndex,
                            PropName = pair.Key,
                            PropValue = (string)pair.Value,
                        };

                        txAction(in transformArg);
                        ++columnIndex;
                    }

                    return outObjDict;
                });
        }

        private struct TxArg
        {
            public IDictionary<string, object> OrigObjDict;
            public int ColumnIndex;
            public string PropName;
            public string PropValue;
            public IDictionary<string, object> OutObjDict;
        }
    }
}