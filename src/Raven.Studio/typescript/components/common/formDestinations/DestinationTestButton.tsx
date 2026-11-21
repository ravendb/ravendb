import { ComponentProps } from "react";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import IconName from "typings/server/icons";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { DestinationTestState } from "./useDestinationTest";

type ButtonProps = Omit<ComponentProps<typeof ButtonWithSpinner>, "type" | "icon" | "onClick" | "isSpinning">;

interface DestinationTestButtonProps extends ButtonProps {
    asyncTest: Pick<DestinationTestState, "execute" | "loading" | "result">;
}

interface TestResultView {
    variant: ButtonProps["variant"];
    icon: IconName;
    label: string;
}

export default function DestinationTestButton({
    asyncTest,
    variant = "info",
    title,
    children,
    ...rest
}: DestinationTestButtonProps) {
    const t = useStudioTranslation("formDestinations");

    const resultView = getResultView(asyncTest.result, t);

    return (
        <ButtonWithSpinner
            type="button"
            variant={resultView?.variant ?? variant}
            icon={resultView?.icon ?? "rocket"}
            title={title ?? (resultView ? t("testCredentials") : undefined)}
            onClick={asyncTest.execute}
            isSpinning={asyncTest.loading}
            {...rest}
        >
            {resultView?.label ?? children ?? t("testCredentials")}
        </ButtonWithSpinner>
    );
}

function getResultView(
    result: DestinationTestState["result"],
    t: ReturnType<typeof useStudioTranslation<"formDestinations">>
): TestResultView | null {
    if (result?.Success) {
        return { variant: "success", icon: "check", label: t("testSuccess") };
    }
    if (result?.Error) {
        return { variant: "danger", icon: "warning", label: t("testFailure") };
    }
    return null;
}
