import { rtlRender } from "test/rtlTestUtils";
import { act } from "@testing-library/react";
import { FormProvider, useForm } from "react-hook-form";
import { AddAnotherNode } from "components/setupWizard/steps/SetupWizardNodeAddressStep";
import { SetupWizardFormData } from "components/setupWizard/setupWizardValidation";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";

type SetupWizardNode = SetupWizardFormData["nodeAddressStep"]["nodes"][number];

function AddAnotherNodeWithoutLicense({ nodes }: { nodes: SetupWizardNode[] }) {
    const form = useForm<SetupWizardFormData>({
        defaultValues: { ...setupWizardFormDefaultValues, nodeAddressStep: { nodes } },
    });

    return (
        <FormProvider {...form}>
            <AddAnotherNode onAddNode={() => null} />
        </FormProvider>
    );
}

describe("AddAnotherNode", () => {
    beforeEach(() => {
        jest.useFakeTimers({ advanceTimers: true });
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    async function hoverAddNodeButton(nodes: SetupWizardNode[]) {
        const { screen, user } = rtlRender(<AddAnotherNodeWithoutLicense nodes={nodes} />);

        const addNodeButton = screen.getByRole("button", { name: /Add another node/ });
        await user.hover(addNodeButton);
        await act(async () => {
            jest.runOnlyPendingTimers();
        });

        return { screen, addNodeButton };
    }

    it("does not show the license limit popover when no node is configured", async () => {
        const { screen, addNodeButton } = await hoverAddNodeButton([]);

        expect(addNodeButton).toBeEnabled();
        expect(screen.queryByText(/single-node cluster/)).not.toBeInTheDocument();
    });

    it("shows the license limit popover when the only allowed node is configured", async () => {
        const { screen, addNodeButton } = await hoverAddNodeButton([{ nodeTag: "A" }]);

        expect(addNodeButton).toBeDisabled();
        expect(screen.getByText(/single-node cluster/)).toBeInTheDocument();
    });
});
