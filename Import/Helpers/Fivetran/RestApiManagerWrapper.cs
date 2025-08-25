using FivetranClient;
using System.Runtime.InteropServices;

namespace Import.Helpers.Fivetran;

public class RestApiManagerWrapper(RestApiManager restApiManager, string groupId) : IDisposable
{
    private readonly RestApiManager _restApiManager = restApiManager;
    private readonly string _groupId = groupId;
    private bool _disposed = false;

    public RestApiManager RestApiManager => _restApiManager;
    public string GroupId => _groupId;

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        if (disposing)
        {
            _restApiManager.Dispose();
        }
        _disposed = true;
    }

    ~RestApiManagerWrapper() => Dispose(false);
}