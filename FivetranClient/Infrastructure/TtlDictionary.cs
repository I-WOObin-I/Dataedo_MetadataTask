namespace FivetranClient.Infrastructure;

public class TtlDictionary<TKey, TValue> where TKey : notnull
{
    private readonly Dictionary<TKey, (TValue Value, DateTime Expiry)> _dictionary = new();
    private readonly object _lock = new();

    public TValue GetOrAdd(TKey key, Func<TValue> valueFactory, TimeSpan ttl)
    {
        lock (_lock)
        {
            if (_dictionary.TryGetValue(key, out var entry))
                {
                    if (DateTime.UtcNow < entry.Expiry)
                    {
                        return entry.Value;
                    }
                    _dictionary.Remove(key);
                }

            var value = valueFactory();
            _dictionary[key] = (value, DateTime.UtcNow.Add(ttl));
            return value;
        }
    }

    public async Task<TValue> GetOrAddAsync(TKey key, Func<Task<TValue>> valueFactory, TimeSpan ttl)
    {
        lock (_lock)
        {
            if (_dictionary.TryGetValue(key, out var entry))
            {
                if (DateTime.UtcNow < entry.Expiry)
                {
                    return entry.Value;
                }
                _dictionary.Remove(key);
            }
        }
        var value = await valueFactory().ConfigureAwait(false);
        lock (_lock)
        {
            _dictionary[key] = (value, DateTime.UtcNow.Add(ttl));
        }
        return value;
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        lock (_lock)
        {
            if (_dictionary.TryGetValue(key, out var entry) && DateTime.UtcNow < entry.Expiry)
            {
                value = entry.Value;
                return true;
            }

            value = default!;
            return false;
        }
    }

    public void RemoveExpiredEntries()
    {
        lock (_lock)
        {
            var now = DateTime.UtcNow;
            var expiredKeys = new List<TKey>();
            foreach (var kvp in _dictionary)
            {
                if (now >= kvp.Value.Expiry)
                {
                    expiredKeys.Add(kvp.Key);
                }
            }
            foreach (var key in expiredKeys)
            {
                _dictionary.Remove(key);
            }
        }
    }
}