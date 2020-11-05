namespace CsvHelperExtensions
{
    public delegate void ActionIn<T>(in T item) where T : struct;
}