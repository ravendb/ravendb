import React from "react";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
} from "components/common/RichPanel";
import { Button, Collapse, InputGroup, Label } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FormInput } from "components/common/Form";
import { HStack } from "components/common/HStack";
import { useForm } from "react-hook-form";

interface IntegrationsConfigPanelProps {
    isDatabaseAdmin: boolean;
    panelCollapsed: boolean;
}

export default function IntegrationsConfigPanel(props: IntegrationsConfigPanelProps) {
    const { isDatabaseAdmin, panelCollapsed } = props;
    const { control } = useForm<null>({});

    return (
        <RichPanel className="flex-row">
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>New credentials</RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        {isDatabaseAdmin && (
                            <>
                                {!panelCollapsed && (
                                    <>
                                        <Button color="success" title="Save credentials">
                                            <Icon icon="save" />
                                            Save credentials
                                        </Button>
                                        <Button color="secondary" title="Discard changes">
                                            <Icon icon="cancel" />
                                            Discard
                                        </Button>
                                    </>
                                )}
                                {panelCollapsed && (
                                    <Button color="danger" title="Delete this script">
                                        <Icon icon="trash" margin="m-0" />
                                    </Button>
                                )}
                            </>
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                <Collapse isOpen={!panelCollapsed}>
                    <RichPanelDetails className="vstack gap-3 p-4">
                        <InputGroup className="vstack mb-1">
                            <Label>Username</Label>
                            <FormInput
                                name="username"
                                control={control}
                                type="text"
                                placeholder="Enter your username"
                                autoComplete="off"
                            />
                        </InputGroup>
                        <InputGroup className="vstack">
                            <Label>Password</Label>
                            <HStack className="gap-1">
                                <div className="position-relative flex-grow">
                                    <FormInput
                                        name="password"
                                        control={control}
                                        type="password"
                                        placeholder="Enter your password"
                                        passwordPreview
                                    />
                                </div>
                                <Button title="Generate a random password">
                                    <Icon icon="random" />
                                    Generate password
                                </Button>
                                <Button title="Copy to clipboard">
                                    <Icon icon="copy-to-clipboard" margin="m-0" />
                                </Button>
                            </HStack>
                        </InputGroup>
                    </RichPanelDetails>
                </Collapse>
            </div>
        </RichPanel>
    );
}
