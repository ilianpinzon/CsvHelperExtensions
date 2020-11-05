using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using NUnit.Framework;

namespace CsvHelperExtensions.Tests
{
    public sealed class DynamicRecordExtensionsTests
    {
        [Test]
        public void Can_drop_column()
        {
            var records = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "5", "6" }
                });

            records = records.DropColumn("B").ToList();

            var expectedRecords = CreateRecords(
                new[] {         "A", "C" },
                new[] { new[] { "1", "3" },
                        new[] { "4", "6" }
                });

            Assert.That(RecordsEqual(records, expectedRecords));
        }

        [Test]
        public void Can_drop_columns()
        {
            var records = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "5", "6" }
                });

            records = records.DropColumns(1, 2).ToList();

            var expectedRecords = CreateRecords(
                new[] {         "A" },
                new[] { new[] { "1" },
                        new[] { "4" }
                });

            Assert.That(RecordsEqual(records, expectedRecords));
        }

        [Test]
        public void Can_fill_column()
        {
            var records = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "5", "6" }
                });

            records = records.FillColumn("B", "X", s => s == "5").ToList();

            var expectedRecords = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "X", "6" }
                });

            Assert.That(RecordsEqual(records, expectedRecords));
        }

        [Test]
        public void Can_insert_column_at_start()
        {
            var records = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "5", "6" }
                });

            records = records.InsertColumn(0, "Z", _ => "0").ToList();

            var expectedRecords = CreateRecords(
                new[] {         "Z", "A", "B", "C" },
                new[] { new[] { "0", "1", "2", "3" },
                        new[] { "0", "4", "5", "6" }
                });

            Assert.That(RecordsEqual(records, expectedRecords));
        }

        [Test]
        public void Can_insert_column_in_middle()
        {
            var records = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "5", "6" }
                });

            records = records.InsertColumn(1, "Z", _ => "0").ToList();

            var expectedRecords = CreateRecords(
                new[] {         "A", "Z", "B", "C" },
                new[] { new[] { "1", "0", "2", "3" },
                        new[] { "4", "0", "5", "6" }
                });

            Assert.That(RecordsEqual(records, expectedRecords));
        }

        [Test]
        public void Can_insert_column_at_end()
        {
            var records = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "5", "6" }
                });

            records = records.InsertColumn(3, "Z", _ => "0").ToList();

            var expectedRecords = CreateRecords(
                new[] {         "A", "B", "C", "Z" },
                new[] { new[] { "1", "2", "3", "0" },
                        new[] { "4", "5", "6", "0" }
                });

            Assert.That(RecordsEqual(records, expectedRecords));
        }

        [Test]
        public void Can_replace_headers()
        {
            var records = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "5", "6" }
                });

            records = records.ReplaceColumnHeaders("X", "Y", "Z").ToList();

            var expectedRecords = CreateRecords(
                new[] {         "X", "Y", "Z" },
                new[] { new[] { "1", "2", "3" },
                        new[] { "4", "5", "6" }
                });

            Assert.That(RecordsEqual(records, expectedRecords));
        }

        [Test]
        public void Can_replace_headers_and_add_new_ones()
        {
            var records = CreateRecords(
                new[] {         "A", "B", "C" },
                new[] { new[] { "1", "2", "3" },
                    new[] { "4", "5", "6" }
                });

            records = records.ReplaceColumnHeaders("X", "Y", "Z", "A", "B", "C").ToList();

            var expectedRecords = CreateRecords(
                new[] {         "X", "Y", "Z", "A", "B", "C" },
                new[] { new[] { "1", "2", "3",  "",  "",  "" },
                        new[] { "4", "5", "6",  "",  "",  "" }
                });

            Assert.That(RecordsEqual(records, expectedRecords));
        }

        private static IReadOnlyList<dynamic> CreateRecords(IReadOnlyList<string> columns,
            IReadOnlyList<IReadOnlyList<string>> records)
        {
            var outRecords = new List<dynamic>();

            for (int i = 0; i < records.Count; ++i)
            {
                IDictionary<string, object> record = new ExpandoObject();
                for (int j = 0; j < columns.Count; ++j)
                {
                    record.Add(columns[j], records[i][j]);
                }

                outRecords.Add(record);
            }

            return outRecords;
        }

        private static bool RecordsEqual(IReadOnlyList<dynamic> records1, IReadOnlyList<dynamic> records2)
        {
            if (records1.Count != records2.Count)
                return false;

            for (int i = 0; i < records1.Count; ++i)
            {
                if (!RecordEquals(records1[i], records2[i]))
                    return false;
            }

            return true;
        }

        private static bool RecordEquals(dynamic record1, dynamic record2)
        {
            IDictionary<string, object> record1Dict = record1;
            IDictionary<string, object> record2Dict = record2;

            if (record1Dict.Keys.Count != record2Dict.Keys.Count)
                return false;

            foreach (var pair in record1Dict)
            {
                if (record2Dict[pair.Key] != pair.Value)
                    return false;
            }

            return true;
        }
    }
}