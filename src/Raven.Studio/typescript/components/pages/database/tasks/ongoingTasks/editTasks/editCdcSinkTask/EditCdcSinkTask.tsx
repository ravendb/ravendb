import React from "react";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppUrls } from "hooks/useAppUrls";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";

interface QueryParams {
    taskId?: number;
}

export default function EditCdcSinkTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const { forCurrentDatabase } = useAppUrls();
    const isEditMode = queryParams?.taskId != null;

    return (
        <div className="content-margin">
            <AboutViewHeading
                title={isEditMode ? "Edit CDC Sink task" : "New CDC Sink task"}
                icon="sql-etl"
                marginBottom={4}
            />

            <Button href={forCurrentDatabase.ongoingTasksUrl()} className="rounded-pill" variant="secondary">
                <Icon icon="arrow-left" />
                Back to ongoing tasks
            </Button>

            <Card className="mt-4">
                <Card.Body>
                    <h4>CDC Sink editor placeholder</h4>
                    <p className="mb-0">
                        The Studio UI for CDC Sink configuration has not been implemented yet.
                        {isEditMode ? ` Task ID: ${queryParams.taskId}.` : ""}
                    </p>
                </Card.Body>
            </Card>
        </div>
    );
}
