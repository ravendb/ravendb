import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormGroup, FormSelect, FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { FieldValues, FieldPath, useWatch, UseControllerProps } from "react-hook-form";

interface FormTaskResponsibleNodeProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: UseControllerProps<TFieldValues>["control"];
    isSetName: TName;
    nodeName: TName;
    isPinName: TName;
    isDisabled?: boolean;
}

export function FormTaskResponsibleNode<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    isSetName,
    nodeName,
    isPinName,
    isDisabled,
}: FormTaskResponsibleNodeProps<TFieldValues, TName>) {
    const isSet = useWatch({ control, name: isSetName });
    const responsibleNode = useWatch({ control, name: nodeName });
    const isPin = useWatch({ control, name: isPinName });
    const t = useStudioTranslation("common");

    const activeDatabase = useAppSelector(databaseSelectors.activeDatabase);
    const isDatabaseSharded = activeDatabase?.isSharded ?? false;

    const memberTags = (activeDatabase?.nodes ?? []).filter((x) => x.type === "Member").map((x) => x.tag);
    const isResponsibleNodeNotMember = !!responsibleNode && !memberTags.includes(responsibleNode);

    const memberOptions: SelectOption[] = memberTags.map((tag) => ({
        value: tag,
        label: t("responsibleNode.nodeOption", { nodeTag: tag }),
    }));

    const possibleMentorOptions: SelectOption[] = isResponsibleNodeNotMember
        ? [
              ...memberOptions,
              {
                  value: responsibleNode,
                  label: t("responsibleNode.nodeOption", { nodeTag: responsibleNode, context: "notMember" }),
              },
          ]
        : memberOptions;

    const isSetBlockedByNoNodes = memberTags.length === 0 && !isSet;

    return (
        <FormGroup>
            <FormGroup>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: isDatabaseSharded,
                            message: t("responsibleNode.shardedInfo"),
                        },
                        {
                            isActive: isSetBlockedByNoNodes,
                            message: t("responsibleNode.noNodesInfo"),
                        },
                    ]}
                >
                    <FormSwitch
                        control={control}
                        name={isSetName}
                        disabled={isDisabled || isDatabaseSharded || isSetBlockedByNoNodes}
                    >
                        {t("responsibleNode.setLabel")}
                    </FormSwitch>
                </ConditionalPopover>
            </FormGroup>
            {isSet && (
                <>
                    <FormGroup>
                        <FormSelect
                            control={control}
                            name={nodeName}
                            options={possibleMentorOptions}
                            isDisabled={isDisabled}
                        />
                    </FormGroup>
                    {responsibleNode && (
                        <FormGroup>
                            <FormSwitch
                                control={control}
                                name={isPinName}
                                title={t("responsibleNode.pinTitle")}
                                disabled={isDisabled}
                            >
                                {t("responsibleNode.pinLabel")}
                            </FormSwitch>
                            <RichAlert variant="info" className="mt-2">
                                <StudioTrans
                                    ns="common"
                                    i18nKey={isPin ? "responsibleNode.pinnedInfo" : "responsibleNode.preferredInfo"}
                                    components={{ br: <br /> }}
                                />
                            </RichAlert>
                        </FormGroup>
                    )}
                </>
            )}
        </FormGroup>
    );
}
