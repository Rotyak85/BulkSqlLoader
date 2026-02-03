using System.Collections.Frozen;
using System.Collections.Generic;
using System.Reflection;

namespace Loaders;

/// <summary>
/// Ottimizzato con FrozenDictionary per lookup ultra-veloci e zero allocazioni post-init.
/// FrozenDictionary è immutabile e ottimizzato per read-only scenarios.
/// </summary>
public sealed class TypeAnalyzer<T>
{
    // FrozenDictionary è più veloce di Dictionary per lookup read-only (fino a 30% più veloce)
    // e utilizza meno memoria. È ottimizzato per scenari dove la collection non viene modificata.
    public readonly FrozenDictionary<string, PropertyInfo> PropertiesIndex;

    internal TypeAnalyzer()
    {
        // Utilizzo di collection expression (C# 12+) per creare il dictionary iniziale
        // poi convertiamo a FrozenDictionary per performance ottimali
        var props = typeof(T).GetProperties();
        var dict = new Dictionary<string, PropertyInfo>(props.Length);
        
        // Loop manuale invece di LINQ per evitare allocazioni dell'enumerator
        foreach (var prop in props)
        {
            dict[prop.Name] = prop;
        }
        
        // ToFrozenDictionary ottimizza internamente la struttura per lookup veloci
        PropertiesIndex = dict.ToFrozenDictionary();
    }
}