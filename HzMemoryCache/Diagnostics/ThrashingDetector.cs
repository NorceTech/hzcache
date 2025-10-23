namespace HzCache.Diagnostics
{
    internal class ThrashingDetector(string checksum)
    {
        public int Counter { get; set; } = 0;
        public string Checksum => checksum;
    }
}