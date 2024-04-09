import appUrl from "common/appUrl";
import copyToClipboard from "common/copyToClipboard";
import { Icon } from "components/common/Icon";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { MergeSuggestionsError } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import { Button, Card, Table } from "reactstrap";

interface MergeSuggestionsErrorsCarouselCardProps {
    errors: MergeSuggestionsError[];
}

export default function MergeSuggestionsErrorsCarouselCard(props: MergeSuggestionsErrorsCarouselCardProps) {
    const { errors } = props;

    return (
        <Card>
            <Card className="bg-faded-primary m-1 p-4">
                <div className="text-limit-width">
                    <h2>Merge suggestions errors</h2>
                    The following errors have been encountered when trying to create index merge suggestions.
                </div>
            </Card>

            <div className="p-2">
                <RichPanel hover>
                    <RichPanelHeader className="px-3 py-2 d-block">
                        <Table responsive className="m-0 table-inner-border">
                            <thead>
                                <tr>
                                    <th>
                                        <div className="small-label">Index name</div>
                                    </th>

                                    <th>
                                        <div className="small-label">Error</div>
                                    </th>
                                    <th />
                                </tr>
                            </thead>
                            <tbody>
                                {errors.map((error, idx) => (
                                    <ErrorTableRow key={idx} error={error} />
                                ))}
                            </tbody>
                        </Table>
                    </RichPanelHeader>
                </RichPanel>
            </div>
        </Card>
    );
}

interface ErrorTableRowProps {
    error: MergeSuggestionsError;
}

function ErrorTableRow({ error }: ErrorTableRowProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { value: isStackTraceVisible, toggle: toggleIsStackTraceVisible } = useBoolean(false);

    return (
        <tr>
            <td>
                <div>
                    <a href={appUrl.forEditIndex(error.indexName, databaseName)}>
                        {error.indexName}
                        <Icon icon="newtab" margin="ms-1" />
                    </a>
                </div>
            </td>
            <td>
                <div>
                    <code>
                        {error.message}
                        {isStackTraceVisible && (
                            <>
                                <br />
                                {error.stackTrace}
                            </>
                        )}
                    </code>
                </div>
                <div className="d-flex justify-content-end">
                    <Button color="link" size="sm" onClick={toggleIsStackTraceVisible}>
                        {isStackTraceVisible ? (
                            <>
                                <Icon icon="collapse-vertical" />
                                Hide details
                            </>
                        ) : (
                            <>
                                <Icon icon="expand-vertical" />
                                Show details
                            </>
                        )}
                    </Button>
                </div>
            </td>
            <td>
                <Button
                    color="primary"
                    size="sm"
                    className="rounded-pill"
                    onClick={() =>
                        copyToClipboard.copy(error.message + error.stackTrace, "Error has been copied to clipboard")
                    }
                >
                    <Icon icon="copy-to-clipboard" margin="m-0" />
                </Button>
            </td>
        </tr>
    );
}
