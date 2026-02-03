using System.Collections.Generic;
using System.Reflection;

namespace Loaders;

public class TypeAnalyzer<T>
{
    public readonly Dictionary<string, PropertyInfo> PropertiesIndex;

    internal TypeAnalyzer()
    {
        PropertiesIndex = new Dictionary<string, PropertyInfo>();

        foreach (var prop in typeof(T).GetProperties())
        {
            PropertiesIndex.Add(prop.Name, prop);
        }
    }
}