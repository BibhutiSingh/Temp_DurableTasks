namespace DurableJobPipeline;

public static class PipelineValidator
{
    public static void Validate(PipelineDefinition pipeline)
    {
        if (pipeline.Id == Guid.Empty) throw new InvalidOperationException("Pipeline id is required.");
        if (string.IsNullOrWhiteSpace(pipeline.Name)) throw new InvalidOperationException("Pipeline name is required.");
        var jobs = pipeline.Jobs.ToDictionary(j => j.Id);
        if (jobs.Count != pipeline.Jobs.Count) throw new InvalidOperationException("Duplicate job id detected.");
        foreach (var job in pipeline.Jobs)
        {
            if (job.Id == Guid.Empty) throw new InvalidOperationException($"Job '{job.Name}' has an empty id.");
            if (string.IsNullOrWhiteSpace(job.Name) || string.IsNullOrWhiteSpace(job.JobType)) throw new InvalidOperationException("Job name/type is required.");
            if (job.MaxAttempts < 1) throw new InvalidOperationException($"Job '{job.Name}' must have MaxAttempts >= 1.");
            foreach (var dep in job.DependsOn)
                if (!jobs.ContainsKey(dep)) throw new InvalidOperationException($"Job '{job.Name}' references unknown dependency '{dep}'.");
        }
        foreach (var job in pipeline.Jobs)
        {
            var visited = new HashSet<Guid>();
            if (HasCycle(job.Id, jobs, visited, new HashSet<Guid>()))
                throw new InvalidOperationException($"Pipeline '{pipeline.Name}' contains a dependency cycle.");
        }
    }

    private static bool HasCycle(Guid current, IReadOnlyDictionary<Guid, JobDefinition> jobs, HashSet<Guid> visited, HashSet<Guid> stack)
    {
        if (!visited.Add(current)) return stack.Contains(current);
        stack.Add(current);
        foreach (var dep in jobs[current].DependsOn)
            if (HasCycle(dep, jobs, visited, stack)) return true;
        stack.Remove(current);
        return false;
    }
}
