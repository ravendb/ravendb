import Badge from "react-bootstrap/Badge";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import { Label } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapAzureToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";

export default function Azure() {
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { azure: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("Azure", mapAzureToDto(formValues));
    });

    return (
        <Card className="well">
            <Card.Body>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Azure
                </FormSwitch>
                <Collapse in={formValues.isEnabled} className="vstack gap-2 mt-2">
                    <div>
                        <FormSwitch
                            name={`${fieldBase}.config.isOverrideConfig`}
                            control={control}
                            className="ms-3 w-100"
                            color="secondary"
                        >
                            Override configuration via external script
                        </FormSwitch>
                        {formValues.config.isOverrideConfig ? (
                            <OverrideConfiguration fieldBase={fieldBase} />
                        ) : (
                            <div className="vstack gap-3 mt-2">
                                <div className="mb-2">
                                    <Label className="d-flex gap-1 align-items-center">
                                        Storage container{" "}
                                        <PopoverWithHoverWrapper
                                            message={
                                                <>
                                                    Storage container should be created manually in order for this OLAP
                                                    to work. You can use the{" "}
                                                    <span className="text-info">Test credentials</span> button to verify
                                                    its existence.
                                                </>
                                            }
                                        >
                                            <Icon icon="info" color="info" margin="m-0" />
                                        </PopoverWithHoverWrapper>
                                        {asyncTest.result?.Success ? (
                                            <Badge bg="success" pill>
                                                <Icon icon="check" />
                                                Successfully connected
                                            </Badge>
                                        ) : asyncTest.result?.Error ? (
                                            <Badge bg="danger" pill>
                                                <Icon icon="warning" />
                                                Failed connection
                                            </Badge>
                                        ) : null}
                                    </Label>
                                    <FormInput
                                        name={getName("storageContainer")}
                                        control={control}
                                        placeholder="Enter a storage container"
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <Label>
                                        Remote folder name <small className="text-muted fw-light">(optional)</small>
                                    </Label>
                                    <FormInput
                                        name={getName("remoteFolderName")}
                                        control={control}
                                        placeholder="Enter a remote folder name"
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <Label>Account name</Label>
                                    <FormInput
                                        name={getName("accountName")}
                                        control={control}
                                        placeholder="Enter an account name"
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <Label>Account key</Label>
                                    <FormInput
                                        name={getName("accountKey")}
                                        control={control}
                                        placeholder="Enter an account key"
                                        type="password"
                                        passwordPreview
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="d-flex">
                                    <FlexGrow />
                                    <ButtonWithSpinner
                                        type="button"
                                        variant="secondary"
                                        onClick={asyncTest.execute}
                                        isSpinning={asyncTest.loading}
                                        icon="rocket"
                                    >
                                        Test credentials
                                    </ButtonWithSpinner>
                                </div>
                                {asyncTest.result?.Error && (
                                    <div className="mt-3">
                                        <ConnectionTestResult testResult={asyncTest.result} />
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                </Collapse>
            </Card.Body>
        </Card>
    );
}

const fieldBase = "destinations.azure";

type FormFieldNames = keyof FormDestinations["destinations"]["azure"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
