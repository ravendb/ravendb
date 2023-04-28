import React from "react";
import { Icon } from "components/common/Icon";
import { Button, Card, CardBody, CardHeader, Collapse } from "reactstrap";
import "./CreateSampleData.scss";
import useBoolean from "components/hooks/useBoolean";
import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import database from "models/resources/database";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import copyToClipboard from "common/copyToClipboard";
import appUrl from "common/appUrl";
import Code from "components/common/Code";

interface CreateSampleDataProps {
    db: database;
}

function CreateSampleData({ db }: CreateSampleDataProps) {
    const { tasksService } = useServices();

    const { value: isCodeSampleOpen, toggle: toggleCodeSample } = useBoolean(false);
    const { value: isSampleDataCreated, toggle: toggleCreateSampleData } = useBoolean(false);
    const { value: isDatabaseEmpty, toggle: toggleDatabaseEmpty } = useBoolean(true);

    const asyncSampleDataClasses = useAsync(() => tasksService.getSampleDataClasses(db), []);

    // TODO: onSave createSampleDataCommand
    // TODO: maybe move loading/error logic to helper method

    if (asyncSampleDataClasses.loading) {
        return <LoadingView />;
    }

    if (asyncSampleDataClasses.error) {
        return <LoadError error="Unable to load sample data classes" refresh={asyncSampleDataClasses.execute} />;
    }

    return (
        <div className="sample-data absolute-fill">
            {svgSmoke}
            <div className="scroll">
                <div className="center">
                    <div className="flex-vertical flex-center text-center">
                        <p className="lead text-emphasis margin-bottom">
                            This action populates the database with various collections, .json documents, indexes, and
                            document extensions such as time series, counters, attachments, and revisions.
                        </p>
                        <p className="small">
                            The{" "}
                            <a href="https://ravendb.net/docs" target="_blank">
                                RavenDB documentation
                            </a>{" "}
                            includes numerous examples that are based on this sample data. This is a simple and
                            effective way to familiarize yourself with RavenDB features and experiment with the data.
                        </p>
                        <div className="margin-top margin-bottom-sm">
                            <Button
                                size="lg"
                                className="rounded-pill"
                                color="primary"
                                onClick={() => {
                                    toggleCreateSampleData();
                                    toggleDatabaseEmpty();
                                }}
                                disabled={!isDatabaseEmpty}
                            >
                                <Icon icon={isSampleDataCreated ? "check" : "magic-wand"} />
                                {isSampleDataCreated ? "Sample data created" : "Create sample data"}
                                {/* TODO: Add functionality to the button */}
                            </Button>
                            {!isDatabaseEmpty && !isSampleDataCreated && (
                                <div className="padding padding-xs margin-top-sm text-warning">
                                    <Icon icon="warning" />
                                    Requires an empty database
                                </div>
                            )}
                            {isSampleDataCreated && (
                                <div className="padding padding-xs margin-top-sm">
                                    <a href={appUrl.forDocuments("", db)}>
                                        <Icon icon="arrow-thin-right" />
                                        Go to documents
                                    </a>
                                </div>
                            )}
                        </div>
                        <Button size="sm" className="rounded-pill margin-bottom-md" onClick={toggleCodeSample}>
                            <Icon icon="hash" /> {isCodeSampleOpen ? "Hide" : "Show"} C# classes
                        </Button>
                    </div>
                    <Collapse isOpen={isCodeSampleOpen}>
                        <Card className="sample-code">
                            <CardBody>
                                <CardHeader className="sample-code-header">
                                    <h3>Sample data C# code</h3>
                                    <Button
                                        className="rounded-pill"
                                        onClick={() =>
                                            copyToClipboard.copy(
                                                asyncSampleDataClasses.result,
                                                "Copied C# classes to clipboard."
                                            )
                                        }
                                    >
                                        <Icon icon="copy" /> <span>Copy C# classes</span>
                                    </Button>
                                </CardHeader>
                                <Code code={asyncSampleDataClasses.result} language="csharp" />
                            </CardBody>
                        </Card>
                    </Collapse>
                </div>
            </div>
        </div>
    );
}

// TODO move to file
const svgSmoke = (
    <svg
        version="1.1"
        xmlns="http://www.w3.org/2000/svg"
        x="0px"
        y="0px"
        viewBox="0 0 1080 640"
        className="sample-data-bg"
    >
        <polygon className="st0" points="649,437 540,471.3 430.3,437 539.3,405.9 		" />
        <path
            className="st1"
            d="M540,474.3c-0.3,0-0.6,0-0.9-0.1l-109.7-34.3c-1.3-0.4-2.1-1.6-2.1-2.9s0.9-2.5,2.2-2.8l109-31.1
            c0.5-0.2,1.1-0.2,1.6,0l109.7,31.1c1.3,0.4,2.2,1.5,2.2,2.8s-0.8,2.5-2.1,2.9l-109,34.3C540.6,474.3,540.3,474.3,540,474.3z
            M440.8,437.1l99.2,31.1l98.5-31L539.3,409L440.8,437.1z"
        />
        <path
            className="smoke-color"
            d="M1031.5-3.5C708.5-3.4,339.2-3.2,16.3-3C3.5,61.5,35,134.4,95.5,189.9c41.3,37.9,100.3,45.7,100.3,45.7
            c42.6,10.9,66.2,7,83.6,27.4c21.6,25.3,0.9,49.5,20,71.6c28.4,33,95.4,9.8,128.8,33.6c20.8,14.9,27.5,42.4,27.5,42.4
            c4.9,13.9,6.1,26.2,6.2,34.5c26,8.4,52,16.8,78,25.2c24.2-7.9,48.5-15.8,72.7-23.7c-0.7-1.8-1.6-4.5-2-7.8
            c-1.7-12.2,3.7-21.9,6.5-26.8c6-10.6,15.5-19.3,25.7-24.7c18.1-9.5,70.4,17.5,110.3-12.4c53.8-40.4,43.9-60.4,58.8-93.9
            c0,0,7.3-15.2,18.4-29.8c31.7-41.9,111-39.2,153.8-67.6C1044.2,144,1067.8,98.1,1031.5-3.5z"
        />
        <path
            className="smoke-color-2"
            d="M586.6,410c2.6-17.6,8.3-17.6,15.7-36.9c11-28.7-0.6-41.5,5.9-69.5c6.8-29.3,21.9-25.5,35.8-55.7
            c20.1-43.6-2.8-67.6,12.7-126.5c8.4-31.9,23.9-52.9,30.4-61.5C721.9,14,769.3,1,789.6-3C600.5-3,411.4-3,222.3-3
            c-0.8,18.1,1.4,36.2,12.9,50.9c8.5,11,21.3,18.8,34,26.3c43,25.2,52.9,24.6,77.4,41.9c18.3,12.9,29.9,21.2,38.4,38
            c9.8,19.3,4.3,27.5,13.3,45.6c13.1,26.5,24.3,24.5,48.8,44.3c26.7,21.7,29.5,58.1,20.7,88.9c-2.2,7.8-5.1,15.8-3.9,23.8
            c1.5,10.3,9.5,18.8,14.6,28.2c11.4,20.9,8.1,44.6,4.2,68.3c19.1,6,38.2,12.1,57.3,18.1c18.3-5.7,36.6-11.3,54.8-17
            C589.5,443.5,584,427.7,586.6,410z"
        />
    </svg>
);

export default CreateSampleData;
