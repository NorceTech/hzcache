namespace HzCache.Diagnostics
{
    internal class TrashingDetector
    {
        public string? Checksum { get; set; } = null;
        public int Counter { get; set; } = 0;
    }
}