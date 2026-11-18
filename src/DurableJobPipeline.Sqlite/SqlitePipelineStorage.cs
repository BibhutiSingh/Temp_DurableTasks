using System.Text.Json;
using DurableJobPipeline;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;

namespace DurableJobPipeline.Sqlite;

public sealed class SqlitePipelineStorage : IPipelineStorage
{
    private readonly string _connectionString;
    public SqlitePipelineStorage(string connectionString) => _connectionString = connectionString;

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await using var c = new SqliteConnection(_connectionString); await c.OpenAsync(ct);
        await using var cmd = c.CreateCommand();
        cmd.CommandText = """
        CREATE TABLE IF NOT EXISTS Pipelines(Id TEXT PRIMARY KEY, Name TEXT NOT NULL UNIQUE, Definition TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS Runs(RunId TEXT PRIMARY KEY, PipelineId TEXT NOT NULL, PipelineName TEXT NOT NULL, Status INTEGER NOT NULL, CreatedAt TEXT NOT NULL, CompletedAt TEXT, Parameters TEXT NOT NULL, CancellationRequested INTEGER NOT NULL, CancellationReason TEXT);
        CREATE TABLE IF NOT EXISTS Jobs(RunId TEXT NOT NULL, JobId TEXT NOT NULL, Status INTEGER NOT NULL, Output TEXT, Error TEXT, StartedAt TEXT, CompletedAt TEXT, BlobUri TEXT, PRIMARY KEY(RunId, JobId));
        """;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task SavePipelineAsync(PipelineDefinition p, CancellationToken ct = default)
    {
        await using var c = new SqliteConnection(_connectionString); await c.OpenAsync(ct);
        await using var cmd = c.CreateCommand(); cmd.CommandText = "INSERT INTO Pipelines(Id,Name,Definition) VALUES($id,$name,$def) ON CONFLICT(Id) DO UPDATE SET Name=$name,Definition=$def;";
        cmd.Parameters.AddWithValue("$id", p.Id.ToString("N")); cmd.Parameters.AddWithValue("$name", p.Name); cmd.Parameters.AddWithValue("$def", JsonSerializer.Serialize(p, PipelineJson.Options)); await cmd.ExecuteNonQueryAsync(ct);
    }

    public Task<PipelineDefinition?> GetPipelineAsync(Guid id, CancellationToken ct = default) => ReadPipelineAsync("Id", id.ToString("N"), ct);
    public Task<PipelineDefinition?> GetPipelineByNameAsync(string name, CancellationToken ct = default) => ReadPipelineAsync("Name", name, ct);

    private async Task<PipelineDefinition?> ReadPipelineAsync(string field, string value, CancellationToken ct)
    {
        await using var c = new SqliteConnection(_connectionString); await c.OpenAsync(ct);
        await using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT Definition FROM Pipelines WHERE {field}=$v LIMIT 1"; cmd.Parameters.AddWithValue("$v", value);
        var x = await cmd.ExecuteScalarAsync(ct); return x is string s ? JsonSerializer.Deserialize<PipelineDefinition>(s, PipelineJson.Options) : null;
    }

    public async Task CreateRunAsync(PipelineRunRecord r, CancellationToken ct = default)
    {
        await using var c = new SqliteConnection(_connectionString); await c.OpenAsync(ct); await using var cmd = c.CreateCommand();
        cmd.CommandText = "INSERT OR IGNORE INTO Runs VALUES($id,$pid,$pn,$s,$ca,$co,$params,$cr,$reason)";
        cmd.Parameters.AddWithValue("$id", r.RunId.ToString("N")); cmd.Parameters.AddWithValue("$pid", r.PipelineId.ToString("N")); cmd.Parameters.AddWithValue("$pn", r.PipelineName); cmd.Parameters.AddWithValue("$s", (int)r.Status); cmd.Parameters.AddWithValue("$ca", r.CreatedAt.ToString("O")); cmd.Parameters.AddWithValue("$co", r.CompletedAt?.ToString("O") ?? (object)DBNull.Value); cmd.Parameters.AddWithValue("$params", r.Parameters.GetRawText()); cmd.Parameters.AddWithValue("$cr", r.CancellationRequested ? 1 : 0); cmd.Parameters.AddWithValue("$reason", r.CancellationReason ?? (object)DBNull.Value); await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<PipelineRunRecord?> GetPipelineRunAsync(Guid id, CancellationToken ct = default)
    {
        await using var c = new SqliteConnection(_connectionString); await c.OpenAsync(ct); await using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT PipelineId,PipelineName,Status,CreatedAt,CompletedAt,Parameters,CancellationRequested,CancellationReason FROM Runs WHERE RunId=$id"; cmd.Parameters.AddWithValue("$id", id.ToString("N"));
        await using var r = await cmd.ExecuteReaderAsync(ct); if (!await r.ReadAsync(ct)) return null;
        return new PipelineRunRecord(id, Guid.Parse(r.GetString(0)), r.GetString(1), (PipelineRunStatus)r.GetInt32(2), DateTimeOffset.Parse(r.GetString(3)), r.IsDBNull(4)?null:DateTimeOffset.Parse(r.GetString(4)), JsonSerializer.Deserialize<JsonElement>(r.GetString(5), PipelineJson.Options), r.GetInt32(6)!=0, r.IsDBNull(7)?null:r.GetString(7));
    }

    public async Task SetPipelineRunStatusAsync(Guid id, PipelineRunStatus status, DateTimeOffset? completedAt = null, CancellationToken ct = default)
    {
        await using var c = new SqliteConnection(_connectionString); await c.OpenAsync(ct); await using var cmd = c.CreateCommand(); cmd.CommandText = "UPDATE Runs SET Status=$s,CompletedAt=$co WHERE RunId=$id"; cmd.Parameters.AddWithValue("$s",(int)status); cmd.Parameters.AddWithValue("$co",completedAt?.ToString("O")??(object)DBNull.Value);cmd.Parameters.AddWithValue("$id",id.ToString("N"));await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task SetCancellationRequestedAsync(Guid id, string? reason = null, CancellationToken ct = default)
    { await using var c=new SqliteConnection(_connectionString);await c.OpenAsync(ct);await using var cmd=c.CreateCommand();cmd.CommandText="UPDATE Runs SET CancellationRequested=1,CancellationReason=COALESCE($reason,CancellationReason) WHERE RunId=$id";cmd.Parameters.AddWithValue("$id",id.ToString("N"));cmd.Parameters.AddWithValue("$reason",reason??(object)DBNull.Value);await cmd.ExecuteNonQueryAsync(ct); }
    public async Task<bool> IsCancellationRequestedAsync(Guid id, CancellationToken ct = default) => (await GetPipelineRunAsync(id,ct))?.CancellationRequested == true;

    public async Task SetJobStatusAsync(JobRunRecord j, CancellationToken ct = default)
    { await using var c=new SqliteConnection(_connectionString);await c.OpenAsync(ct);await using var cmd=c.CreateCommand();cmd.CommandText="INSERT INTO Jobs VALUES($r,$j,$s,$o,$e,$st,$co,$b) ON CONFLICT(RunId,JobId) DO UPDATE SET Status=$s,Output=$o,Error=$e,StartedAt=$st,CompletedAt=$co,BlobUri=$b";cmd.Parameters.AddWithValue("$r",j.RunId.ToString("N"));cmd.Parameters.AddWithValue("$j",j.JobId.ToString("N"));cmd.Parameters.AddWithValue("$s",(int)j.Status);cmd.Parameters.AddWithValue("$o",j.Output?.GetRawText()??(object)DBNull.Value);cmd.Parameters.AddWithValue("$e",j.Error??(object)DBNull.Value);cmd.Parameters.AddWithValue("$st",j.StartedAt?.ToString("O")??(object)DBNull.Value);cmd.Parameters.AddWithValue("$co",j.CompletedAt?.ToString("O")??(object)DBNull.Value);cmd.Parameters.AddWithValue("$b",j.BlobUri??(object)DBNull.Value);await cmd.ExecuteNonQueryAsync(ct); }

    public async Task<JobRunRecord?> GetJobRunAsync(Guid r, Guid j, CancellationToken ct=default)
    { var all=await GetJobRunsAsync(r,ct);return all.FirstOrDefault(x=>x.JobId==j); }

    public async Task<IReadOnlyList<JobRunRecord>> GetJobRunsAsync(Guid runId, CancellationToken ct=default)
    {
        var list=new List<JobRunRecord>();await using var c=new SqliteConnection(_connectionString);await c.OpenAsync(ct);await using var cmd=c.CreateCommand();cmd.CommandText="SELECT JobId,Status,Output,Error,StartedAt,CompletedAt,BlobUri FROM Jobs WHERE RunId=$r";cmd.Parameters.AddWithValue("$r",runId.ToString("N"));await using var rd=await cmd.ExecuteReaderAsync(ct);
        while(await rd.ReadAsync(ct)){JsonElement? o=rd.IsDBNull(2)?null:JsonSerializer.Deserialize<JsonElement>(rd.GetString(2),PipelineJson.Options); DateTimeOffset? T(int i)=>rd.IsDBNull(i)?null:DateTimeOffset.Parse(rd.GetString(i));list.Add(new JobRunRecord(runId,Guid.Parse(rd.GetString(0)),(JobStatus)rd.GetInt32(1),o,rd.IsDBNull(3)?null:rd.GetString(3),T(4),T(5),rd.IsDBNull(6)?null:rd.GetString(6)));}return list;
    }
}

public static class SqliteRegistration
{
    public static IServiceCollection AddDurableJobPipelineSqlite(this IServiceCollection services, string connectionString)
    { services.AddSingleton<IPipelineStorage>(_=>new SqlitePipelineStorage(connectionString)); return services; }
}
