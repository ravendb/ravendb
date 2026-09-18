import ExpandableListContainer from "components/common/ExpandableListContainer";
import RichAlert from "components/common/RichAlert";
import useBoolean from "components/hooks/useBoolean";
import EditCdcSinkTaskWarningMessage from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskWarningMessage";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import CdcTestResult = Raven.Client.Documents.Operations.CdcSink.Test.CdcTestResult;

interface EditCdcSinkTaskVerificationAlertProps {
    result: CdcTestResult;
    className?: string;
}

export default function EditCdcSinkTaskVerificationAlert({ result, className }: EditCdcSinkTaskVerificationAlertProps) {
    if (!result || (result.Success && result.Warnings.length === 0)) {
        return null;
    }

    return (
        <div className={classNames("vstack gap-2", className)}>
            {!result.Success && (
                <RichAlert
                    variant="danger"
                    title="Data source verification failed for the configured tables."
                    className="mb-0"
                >
                    <VerificationErrorItem error={result.Error} />
                </RichAlert>
            )}
            {result.Warnings.length > 0 && (
                <RichAlert
                    variant="warning"
                    title={result.Success ? "Data source verification passed with warnings." : null}
                    className="mb-0"
                >
                    <ExpandableListContainer
                        items={result.Warnings}
                        renderItem={(warning) => <EditCdcSinkTaskWarningMessage message={warning} />}
                    />
                </RichAlert>
            )}
        </div>
    );
}

function VerificationErrorItem({ error }: { error: string }) {
    const { value: isDetailsShown, toggle: toggleDetails } = useBoolean(false);
    const formattedError = formatDryRunError(error);

    return (
        <div>
            <EditCdcSinkTaskWarningMessage message={formattedError.message} />
            {formattedError.details && (
                <>
                    <Button variant="link" size="sm" className="p-0" onClick={toggleDetails}>
                        {isDetailsShown ? "Hide details" : "Show details"}
                    </Button>
                    {isDetailsShown && (
                        <pre className="small text-break mb-0 mt-1" style={{ whiteSpace: "pre-wrap" }}>
                            {formattedError.details}
                        </pre>
                    )}
                </>
            )}
        </div>
    );
}

export interface CdcSinkVerificationError {
    message: string;
    details?: string;
}

export function formatDryRunError(error: string): CdcSinkVerificationError {
    const [firstLine, ...rest] = (error ?? "").split(/\r?\n/);
    const exceptionTypePrefix = /^[\w.`]+(Exception|Error):\s*/;
    const message = firstLine?.replace(exceptionTypePrefix, "").trim() || "The CDC dry run failed.";
    const details = rest.join("\n").trim();

    return details ? { message, details } : { message };
}
