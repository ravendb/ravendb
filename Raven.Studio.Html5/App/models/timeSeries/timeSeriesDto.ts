interface timeSeriesKeyDto {
    Prefix: string;
    Key: string;
    Count: number;
}

interface timeSeriesPointDto {
    Prefix: string;
    Key: string;
    At: number;
    Values: number[];
}
