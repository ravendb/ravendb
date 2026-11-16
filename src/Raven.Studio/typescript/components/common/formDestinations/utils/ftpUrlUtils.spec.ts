import { getFtpProtocol, isSupportedFtpUrl } from "./ftpUrlUtils";

describe("getFtpProtocol", () => {
    it.each([
        ["ftp://host", "ftp"],
        ["ftps://host", "ftps"],
        ["FTPS://host", "ftps"],
        ["  ftps://host  ", "ftps"],
        ["host", null],
        ["", null],
        [null, null],
    ])("reads %p as %p", (url, expected) => {
        expect(getFtpProtocol(url)).toBe(expected);
    });
});

describe("isSupportedFtpUrl", () => {
    it.each([
        ["ftp://host/backups", true],
        ["FTPS://host", true],
        ["host.name:21/backups", true],
        ["http://host", false],
        ["sftp://host", false],
        ["   ", false],
        [null, false],
    ])("reads %p as %p", (url, expected) => {
        expect(isSupportedFtpUrl(url)).toBe(expected);
    });
});
