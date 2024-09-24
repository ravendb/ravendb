import rqlLanguageService from "common/rqlLanguageService";
import AceEditor from "components/common/AceEditor";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import queryCriteria from "models/database/query/queryCriteria";
import { useMemo, useState } from "react";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { Card, InputGroup, Label } from "reactstrap";
import { todo } from "common/developmentHelper";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import DatabaseCustomSorterTestResult from "components/pages/database/settings/customSorters/DatabaseCustomSorterTestResult";

interface DatabaseCustomSorterTestProps {
    name: string;
}

todo("Feature", "Damian", "Show collections selector");

export default function DatabaseCustomSorterTest({ name }: DatabaseCustomSorterTestProps) {
    const db = useAppSelector(databaseSelectors.activeDatabase);

    const { databasesService } = useServices();

    const [testRql, setTestRql] = useState(`from index <indexName>\r\norder by custom(<fieldName>, "${name}")`);

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
            <div>{asyncTest.status === "success" && <DatabaseCustomSorterTestResult result={asyncTest.result} />}</div>
        </Card>
    );
}
