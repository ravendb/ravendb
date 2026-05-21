import ExpandableListContainer from "components/common/ExpandableListContainer";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import RichAlert from "components/common/RichAlert";
import { UseAsyncReturn } from "react-async-hook";

interface EditCdcSinkTaskVerifyResultProps {
    asyncVerifySource: UseAsyncReturn<Raven.Server.Documents.CdcSink.CdcSinkVerificationResult, []>;
}

export default function EditCdcSinkTaskVerifyResult({ asyncVerifySource }: EditCdcSinkTaskVerifyResultProps) {
    if (asyncVerifySource.status === "not-requested") {
        return null;
    }

    if (asyncVerifySource.status === "loading") {
        return <LoadingView />;
    }

    if (asyncVerifySource.status === "error") {
        return <LoadError error="Unable to verify source connection" refresh={asyncVerifySource.execute} />;
    }

    const result = asyncVerifySource.result;

    if (!result.Success) {
        return (
            <div>
                {!result.HasPermissionToSetup && (
                    <RichAlert variant="danger">
                        The RavenDB server does not have permissions to set up the CDC Sink task. Please make sure that
                        the server has the required permissions and try again.
                    </RichAlert>
                )}
                {result.Errors?.length > 0 && (
                    <RichAlert variant="danger">
                        <ExpandableListContainer items={result.Errors} renderItem={(err) => err} />
                    </RichAlert>
                )}
                {result.Warnings?.length > 0 && (
                    <RichAlert variant="warning">
                        <ExpandableListContainer items={result.Warnings} renderItem={(warn) => warn} />
                    </RichAlert>
                )}
            </div>
        );
    }

    return (
        <RichAlert variant="success" onCancel={asyncVerifySource.reset}>
            Connection verified successfully
        </RichAlert>
    );
}
