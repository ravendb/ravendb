import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { PropSummary, PropSummaryItem, PropSummaryName, PropSummaryValue } from "components/common/PropSummary";
import React from "react";
import { UncontrolledPopover } from "reactstrap";
import { CreateDatabaseRegularFormData } from "./createDatabaseRegularValidation";

interface QuickCreateButtonProps {
    formValues: CreateDatabaseRegularFormData;
    isSubmitting: boolean;
}

export default function QuickCreateButton({ formValues, isSubmitting }: QuickCreateButtonProps) {
    return (
        <>
            <ButtonWithSpinner
                type="submit"
                className="rounded-pill me-1"
                id="quickCreateButton"
                icon="star"
                isSpinning={isSubmitting}
                title="Quick Create (Ctrl + Enter)"
            >
                Quick Create
            </ButtonWithSpinner>

            <UncontrolledPopover placement="top" target="quickCreateButton" trigger="hover" className="bs5">
                <PropSummary>
                    <PropSummaryItem>
                        <PropSummaryName>
                            <Icon icon="encryption" /> Encryption
                        </PropSummaryName>
                        {formValues.basicInfoStep.isEncrypted ? (
                            <PropSummaryValue color="success"> ON</PropSummaryValue>
                        ) : (
                            <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                        )}
                    </PropSummaryItem>

                    <PropSummaryItem>
                        <PropSummaryName>
                            <Icon icon="replication" /> Replication
                        </PropSummaryName>
                        {formValues.replicationAndShardingStep.replicationFactor > 1 ? (
                            <PropSummaryValue color="success"> ON</PropSummaryValue>
                        ) : (
                            <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                        )}
                    </PropSummaryItem>

                    <PropSummaryItem>
                        <PropSummaryName>
                            <Icon icon="sharding" /> Sharding
                        </PropSummaryName>
                        {formValues.replicationAndShardingStep.isSharded ? (
                            <PropSummaryValue color="success"> ON</PropSummaryValue>
                        ) : (
                            <PropSummaryValue color="danger"> OFF</PropSummaryValue>
                        )}
                    </PropSummaryItem>

                    {formValues.replicationAndShardingStep.isManualReplication && (
                        <PropSummaryItem>
                            <PropSummaryName>
                                <Icon icon="node" /> Manual node selection
                            </PropSummaryName>
                            <PropSummaryValue color="success"> ON</PropSummaryValue>
                        </PropSummaryItem>
                    )}

                    <PropSummaryItem>
                        <PropSummaryName>
                            {formValues.dataDirectoryStep.isDefault ? (
                                <>
                                    <Icon icon="path" /> <strong>Default</strong> path
                                </>
                            ) : (
                                <>
                                    <Icon icon="path" /> <strong className="text-success">Custom</strong> path
                                </>
                            )}
                        </PropSummaryName>
                    </PropSummaryItem>
                </PropSummary>
            </UncontrolledPopover>
        </>
    );
}
