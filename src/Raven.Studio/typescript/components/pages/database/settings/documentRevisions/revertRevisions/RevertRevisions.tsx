import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { NonShardedViewProps } from "components/models/common";
import { useAppSelector } from "components/store";
import React from "react";
import { useForm, useWatch } from "react-hook-form";
import { RevertRevisionsFormData, revertRevisionsYupResolver } from "./RevertRevisionsValidation";
import { Row, Col, Form, Card, CardBody, Label, FormGroup, InputGroup } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAppUrls } from "components/hooks/useAppUrls";
import { Icon } from "components/common/Icon";
import { FormInput, FormSelect } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import assertUnreachable from "components/utils/assertUnreachable";
import { DevTool } from "@hookform/devtools";
import moment from "moment";
import useConfirm from "components/hooks/useConfirm";
import { tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import notificationCenter from "common/notifications/notificationCenter";
import genUtils from "common/generalUtils";
import { todo } from "common/developmentHelper";
import FormCollectionsSelect from "components/common/FormCollectionsSelect";

todo("Styling", "Kwiato", "input type date styling");
todo("Styling", "Kwiato", "input and validation errors position");

export default function RevertRevisions({ db }: NonShardedViewProps) {
    const { control, formState, handleSubmit, setValue } = useForm<RevertRevisionsFormData>({
        resolver: revertRevisionsYupResolver,
        defaultValues: {
            pointInTime: null,
            timeWindow: null,
            timeMagnitude: "hours",
            isRevertAllCollections: true,
            collections: [],
        },
    });

    const { isRevertAllCollections, collections, timeMagnitude, pointInTime } = useWatch({ control });

    const { forCurrentDatabase } = useAppUrls();

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const formattedPointInTimeUtc = moment(pointInTime).utc().format(defaultDateFormat) + " UTC";

    const [RevertConfirm, confirmRevert] = useConfirm({
        title: `Do you want to revert documents state to date: ${formattedPointInTimeUtc}?`,
        icon: "revert-revisions",
        actionColor: "primary",
        confirmText: "Revert",
    });

    const { databasesService } = useServices();

    const onRevert = async (formData: RevertRevisionsFormData) => {
        if (await confirmRevert()) {
            return tryHandleSubmit(async () => {
                const result = await databasesService.revertRevisions(db, toDto(formData));
                notificationCenter.instance.openDetailsForOperationById(db, result.OperationId);
            });
        }
    };

    return (
        <Row className="content-margin gy-sm">
            <DevTool control={control} />
            <Col>
                <AboutViewHeading title="Revert Revisions" icon="revert-revisions" />
                <Form onSubmit={handleSubmit(onRevert)}>
                    <div className="d-flex justify-content-between align-items-end">
                        <RevertConfirm />
                        <ButtonWithSpinner
                            type="submit"
                            color="primary"
                            icon="revert-revisions"
                            disabled={!formState.isDirty}
                            isSpinning={formState.isSubmitting}
                        >
                            Revert
                        </ButtonWithSpinner>
                        <small>
                            <a href={forCurrentDatabase.revisions()} title="Navigate to Document Revisions View">
                                <Icon icon="link" />
                                Go back to Revisions View
                            </a>
                        </small>
                    </div>
                    <Card className="mt-3">
                        <CardBody className="gap-4">
                            <FormGroup className="w-50">
                                <Label for="pointInTime">Point in Time</Label>
                                <FormInput
                                    type="datetime-local"
                                    control={control}
                                    id="pointInTime"
                                    name="pointInTime"
                                    max={moment().endOf("day").format(genUtils.inputDateTimeFormat)}
                                    addonText="local"
                                />
                            </FormGroup>
                            <FormGroup className="w-50">
                                <Label for="timeWindow">Time Window</Label>
                                <InputGroup>
                                    <FormInput
                                        type="number"
                                        control={control}
                                        id="timeWindow"
                                        name="timeWindow"
                                        placeholder={`default (${getTimeWindowPlaceholder(timeMagnitude)})`}
                                    />
                                    <FormSelect
                                        control={control}
                                        name="timeMagnitude"
                                        options={timeWindowOptions}
                                        isSearchable={false}
                                    />
                                </InputGroup>
                            </FormGroup>
                        </CardBody>
                    </Card>
                    {isDatabaseAdmin && (
                        <Card className="mt-3">
                            <CardBody>
                                <FormCollectionsSelect
                                    control={control}
                                    collectionsFormName="collections"
                                    collections={collections}
                                    isAllCollectionsFormName="isRevertAllCollections"
                                    isAllCollections={isRevertAllCollections}
                                    setValue={setValue}
                                />
                            </CardBody>
                        </Card>
                    )}
                </Form>
            </Col>
            <Col sm={12} lg={4}>
                <AboutViewAnchored defaultOpen="about-view">
                    <AccordionItemWrapper
                        targetId="about-view"
                        icon="about"
                        color="info"
                        description="Get additional info on this feature"
                        heading="About this view"
                    >
                        {pointInTime && (
                            <div className="flex-horizontal margin-bottom">
                                <div>
                                    When &apos;Revert Revisions&apos; is executed the following rules are applied:
                                    <ul>
                                        <li>
                                            Documents
                                            <strong>
                                                <em> modified </em>
                                            </strong>
                                            after Point in Time:
                                            <code> {formattedPointInTimeUtc} </code>
                                            will be reverted (by creating new revision) to latest version before
                                            <code> {formattedPointInTimeUtc} </code>.
                                        </li>
                                        <li>
                                            If collection has maximum revisions limit and all of them were
                                            <strong>
                                                <em> created </em>
                                            </strong>
                                            after Point in Time:
                                            <code> {formattedPointInTimeUtc} </code>
                                            the oldest revision will be used.
                                        </li>
                                        <li>
                                            Documents
                                            <strong>
                                                <em> created </em>
                                            </strong>
                                            after Point in Time:
                                            <code> {formattedPointInTimeUtc} </code>
                                            will be moved to
                                            <strong>
                                                <em> Revisions&nbsp;Bin</em>
                                            </strong>
                                            .
                                        </li>
                                        <li>Remaining documents will not be modified.</li>
                                    </ul>
                                </div>
                            </div>
                        )}
                        <div>
                            <strong>Time Window</strong> parameter is used for performance optimization: since revisions
                            are not sorted by date, we stop the revert process when hitting a versioned document outside
                            the window.
                        </div>
                    </AccordionItemWrapper>
                </AboutViewAnchored>
            </Col>
        </Row>
    );
}

const defaultDateFormat = "DD/MM/YYYY HH:mm";
const defaultWindowValue = 96;

const timeWindowOptions: SelectOption<timeMagnitude>[] = ["minutes", "hours", "days"].map((x: timeMagnitude) => ({
    value: x,
    label: x,
}));

const getTimeWindowPlaceholder = (magnitude: timeMagnitude) => {
    switch (magnitude) {
        case "minutes":
            return "5760";
        case "hours":
            return "96";
        case "days":
            return "4";
        default:
            assertUnreachable(magnitude);
    }
};

function toDto(formData: RevertRevisionsFormData): Raven.Server.Documents.Revisions.RevertRevisionsRequest {
    let WindowInSec = formData.timeWindow ?? defaultWindowValue;

    switch (formData.timeMagnitude) {
        case "minutes":
            WindowInSec *= 60;
            break;
        case "hours":
            WindowInSec *= 3600;
            break;
        case "days":
            WindowInSec *= 24 * 3600;
            break;
        default:
            assertUnreachable(formData.timeMagnitude);
    }

    return {
        Time: moment(formData.pointInTime).utc().toISOString(),
        WindowInSec,
        Collections: formData.isRevertAllCollections ? [] : formData.collections,
    };
}
