import { MethodGroup } from "components/common/sampleQueries/partials/sampleQueriesTypes";
import { contextScriptMethodGroups, updateScriptMethodGroups } from "./editGenAiTaskMethodsData";

const contextOnlySignatures = [
    "ai.genContext(ctx)",
    "AIContextItem.withText(data)",
    "AIContextItem.withPng(data)",
    "AIContextItem.withJpeg(data)",
    "AIContextItem.withWebp(data)",
    "AIContextItem.withGif(data)",
    "AIContextItem.withPdf(data)",
    "loadAttachment(name)",
    "hasAttachment(name)",
    "getAttachments()",
    "getRevisionsCount()",
    "getCounters()",
    "hasCounter(name)",
    "getTimeSeries()",
    "hasTimeSeries(timeSeriesName)",
];

const updateOnlySignatures = [
    "$input",
    "$output",
    "put(id, document[, changeVector])",
    "del(documentId[, changeVector])",
    "archived.archiveAt(document, utcDateString)",
];

function getSignatures(groups: MethodGroup[]) {
    return groups.flatMap((group) => group.methods.map((method) => method.signature));
}

function getSampleScripts(groups: MethodGroup[]) {
    return groups.flatMap((group) => group.methods.map((method) => method.sampleScript).filter(Boolean)).join("\n");
}

describe("editGenAiTaskMethodsData", () => {
    it("shows only methods supported by the context generation script", () => {
        const signatures = getSignatures(contextScriptMethodGroups);

        expect(signatures).toHaveLength(48);
        expect(signatures).toEqual(expect.arrayContaining(contextOnlySignatures));
        expect(signatures).toEqual(expect.not.arrayContaining(updateOnlySignatures));
        expect(contextScriptMethodGroups.every((group) => group.methods.length > 0)).toBe(true);
    });

    it("shows only methods supported by the update script", () => {
        const signatures = getSignatures(updateScriptMethodGroups);

        expect(signatures).toHaveLength(38);
        expect(signatures).toEqual(expect.arrayContaining(updateOnlySignatures));
        expect(signatures).toEqual(expect.not.arrayContaining(contextOnlySignatures));
        expect(updateScriptMethodGroups.every((group) => group.methods.length > 0)).toBe(true);
    });

    it("does not load update-only examples into the context generation script", () => {
        expect(getSampleScripts(contextScriptMethodGroups)).not.toMatch(/\$(input|output)\b/);
    });

    it("does not load context-only examples into the update script", () => {
        expect(getSampleScripts(updateScriptMethodGroups)).not.toMatch(
            /\b(ai\.|loadAttachment|hasAttachment|getAttachments|getRevisionsCount|getCounters|hasCounter|getTimeSeries|hasTimeSeries)\s*\(/
        );
    });
});
