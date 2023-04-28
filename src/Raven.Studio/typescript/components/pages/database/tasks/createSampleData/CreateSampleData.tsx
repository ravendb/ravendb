import React from "react";
import { Icon } from "components/common/Icon";
import { Button, Card, CardBody, CardHeader, Collapse, Spinner } from "reactstrap";
import "./CreateSampleData.scss";
import useBoolean from "components/hooks/useBoolean";
import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import database from "models/resources/database";
import copyToClipboard from "common/copyToClipboard";
import appUrl from "common/appUrl";
import Code from "components/common/Code";
import { LoadError } from "components/common/LoadError";
import SmokeSvg from "./CreateSampleDataSmoke";

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

    return (
        <div className="sample-data absolute-fill">
            <SmokeSvg />
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
                                    {asyncSampleDataClasses.result && (
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
                                    )}
                                </CardHeader>
                                {asyncSampleDataClasses.loading && (
                                    <div className="d-flex justify-content-center">
                                        <Spinner className="spinner-gradient" />
                                    </div>
                                )}
                                {asyncSampleDataClasses.error && (
                                    <LoadError
                                        error="Unable to load sample data classes"
                                        refresh={asyncSampleDataClasses.execute}
                                    />
                                )}
                                {asyncSampleDataClasses.result && (
                                    <Code code={asyncSampleDataClasses.result} language="csharp" />
                                )}
                            </CardBody>
                        </Card>
                    </Collapse>
                </div>
            </div>
        </div>
    );
}

export default CreateSampleData;
