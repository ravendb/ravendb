import { ReactNode } from "react";
import Card from "react-bootstrap/Card";
import Collapse from "react-bootstrap/Collapse";
import { useFormContext, useWatch } from "react-hook-form";
import { FormSwitch } from "components/common/Form";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import DestinationTestButton from "./DestinationTestButton";
import OverrideConfiguration from "./OverrideConfiguration";
import OverrideConfigurationViaExternalScriptToggle from "./OverrideConfigurationViaExternalScriptToggle";
import { DestinationType, FormDestinations } from "./utils/formDestinationsTypes";
import { DestinationTestState } from "./useDestinationTest";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DestinationCardProps {
    type: DestinationType;
    asyncTest?: DestinationTestState;
    children: ReactNode;
}

export default function DestinationCard({ type, asyncTest, children }: DestinationCardProps) {
    const { control } = useFormContext<FormDestinations>();
    const isEnabled = useWatch({ control, name: `destinations.${type}.isEnabled` });
    const isOverrideConfig = useWatch({ control, name: `destinations.${type}.config.isOverrideConfig` });
    const t = useStudioTranslation("formDestinations");

    return (
        <Card className="well">
            <Card.Body>
                <FormSwitch name={`destinations.${type}.isEnabled`} control={control}>
                    {t(`types.${type}`)}
                </FormSwitch>
                <Collapse in={isEnabled} className="vstack gap-2 mt-2">
                    <div>
                        <OverrideConfigurationViaExternalScriptToggle fieldBase={`destinations.${type}`} />
                        {isOverrideConfig ? (
                            <OverrideConfiguration fieldBase={`destinations.${type}`} />
                        ) : (
                            <div className="vstack gap-3 mt-2">
                                {children}
                                {asyncTest && (
                                    <>
                                        <div className="d-flex justify-content-end">
                                            <DestinationTestButton asyncTest={asyncTest} variant="secondary" />
                                        </div>
                                        {asyncTest.result?.Error && (
                                            <ConnectionTestResult testResult={asyncTest.result} />
                                        )}
                                    </>
                                )}
                            </div>
                        )}
                    </div>
                </Collapse>
            </Card.Body>
        </Card>
    );
}
