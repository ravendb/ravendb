import rqlLanguageService from "common/rqlLanguageService";
import AceEditor from "components/common/AceEditor";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import queryCriteria from "models/database/query/queryCriteria";
import React, { useEffect, useMemo, useState } from "react";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { Card, InputGroup, Label, Button, Badge } from "reactstrap";
import documentBasedColumnsProvider from "widgets/virtualGrid/columns/providers/documentBasedColumnsProvider";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import document from "models/database/documents/document";
import VirtualGrid from "components/common/VirtualGrid";
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import { todo } from "common/developmentHelper";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

interface DiagnosticsItem {
    Message: string;
}

type TestResultTab = "results" | "diagnostics";

interface DatabaseCustomSorterTestProps {
    name: string;
}

todo(
    "Feature",
    "Damian",
    "Show virtual grid collections selector",
    "https://issues.hibernatingrhinos.com/issue/RavenDB-22159"
);

export default function DatabaseCustomSorterTest(props: DatabaseCustomSorterTestProps) {
    const { name } = props;

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const [testRql, setTestRql] = useState(`from index <indexName>\r\norder by custom(<fieldName>, "${name}")`);

    const { databasesService } = useServices();

    // Changing the database causes re-mount
    const asyncGetIndexNames = useAsync(async () => {
        const dto = await databasesService.getEssentialStats(db.name);
        return dto?.Indexes?.map((x) => x.Name);
    }, []);

    const asyncTest = useAsyncCallback(() => {
        const criteria = queryCriteria.empty();
        criteria.queryText(testRql);
        criteria.diagnostics(true);

        return databasesService.query({
            db: db.name,
            skip: 0,
            take: 128,
            criteria,
        });
    });

    const [gridController, setGridController] = useState<virtualGridController<document & DiagnosticsItem>>();

    const documentsProvider = useMemo(
        () =>
            new documentBasedColumnsProvider(db.name, gridController, {
                showRowSelectionCheckbox: false,
                showSelectAllCheckbox: false,
                enableInlinePreview: true,
            }),
        [db, gridController]
    );

    const [currentTab, setCurrentTab] = useState<TestResultTab>("results");

    useEffect(() => {
        if (!gridController || asyncTest.status !== "success") {
            return;
        }

        gridController.headerVisible(true);
        gridController.init(
            () => fetcher(asyncTest.result, currentTab),
            (w, r) => {
                if (currentTab === "results") {
                    return documentsProvider.findColumns(w, r, ["__metadata"]);
                }
                if (currentTab === "diagnostics") {
                    return diagnosticsColumnsProvider(gridController);
                }
            }
        );
    }, [gridController, asyncTest.result, asyncTest.status, documentsProvider, currentTab]);

    useEffect(() => {
        gridController?.reset();
    }, [asyncTest.result, gridController, currentTab]);

    const languageService = useMemo(
        () => new rqlLanguageService(db, () => asyncGetIndexNames.result ?? [], "Select"),
        [asyncGetIndexNames.result, db]
    );

    return (
        <Card className="gap-2 p-4">
            <ButtonWithSpinner
                color="primary"
                onClick={asyncTest.execute}
                className="w-fit-content"
                icon="play"
                isSpinning={asyncTest.loading}
            >
                Run test
            </ButtonWithSpinner>
            <InputGroup className="vstack">
                <Label>Enter test RQL:</Label>
                <AceEditor
                    value={testRql}
                    onChange={setTestRql}
                    execute={asyncTest.execute}
                    mode="rql"
                    languageService={languageService}
                    height="100px"
                />
            </InputGroup>
            {asyncTest.status === "success" && (
                <div>
                    <h3>Test Results</h3>
                    <span className="text-muted small">Displaying up to 128 results</span>

                    <div style={{ position: "relative", height: "300px" }}>
                        <VirtualGrid setGridController={setGridController} />
                    </div>

                    <div className="d-flex mt-2 gap-2">
                        <Button
                            size="sm"
                            className="rounded-pill"
                            onClick={() => setCurrentTab("results")}
                            active={currentTab === "results"}
                        >
                            Results
                            <Badge color="primary" className="ms-1">
                                {asyncTest.result.items.length}
                            </Badge>
                        </Button>
                        <Button
                            size="sm"
                            className="rounded-pill"
                            onClick={() => setCurrentTab("diagnostics")}
                            active={currentTab === "diagnostics"}
                        >
                            Diagnostics
                            <Badge color="primary" className="ms-1">
                                {asyncTest.result.additionalResultInfo?.Diagnostics?.length}
                            </Badge>
                        </Button>
                    </div>
                </div>
            )}
        </Card>
    );
}

const fetcher = (results: pagedResultExtended<document>, currentTab: TestResultTab) => {
    if (currentTab === "results") {
        return $.when(results);
    }
    if (currentTab === "diagnostics") {
        return $.when({
            items: results.additionalResultInfo.Diagnostics.map(
                (d: string) =>
                    ({
                        Message: d,
                    }) satisfies DiagnosticsItem
            ),
            totalResultCount: results.additionalResultInfo.Diagnostics.length,
        });
    }
};

const diagnosticsColumnsProvider = (gridController: virtualGridController<DiagnosticsItem>): virtualColumn[] => {
    return [
        new textColumn<DiagnosticsItem>(gridController, (x) => x.Message, "Message", "100%", {
            sortable: "string",
        }),
    ];
};
