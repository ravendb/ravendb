import { RtlScreen, rtlRender } from "test/rtlTestUtils";
import * as Stories from "./CreateDatabase.stories";
import { composeStories } from "@storybook/react";

const { DefaultCreateDatabase } = composeStories(Stories);

describe("CreateDatabase", () => {
    describe("Regular", () => {
        it("can disable encryption when the license does not allow it", async () => {
            const { screen, user } = rtlRender(<DefaultCreateDatabase hasEncryption={false} />);

            const encryptionSwitch = screen.getByLabelText(/Encrypt at Rest/);
            expect(encryptionSwitch).toBeDisabled();

            await user.hover(encryptionSwitch);
            expect(await screen.findByText(/Current license doesn't include/i)).toBeInTheDocument();
        });

        it("can disable encryption when server is not secure", async () => {
            const { screen, user } = rtlRender(<DefaultCreateDatabase isSecureServer={false} />);

            const encryptionSwitch = screen.getByLabelText(/Encrypt at Rest/);
            expect(encryptionSwitch).toBeDisabled();

            await user.hover(encryptionSwitch);
            expect(await screen.findByText(/Authentication is off/i)).toBeInTheDocument();
        });

        it("can disable dynamic database distribution when the license does not allow it", async () => {
            const { screen, user, fillInput, fireClick } = rtlRender(
                <DefaultCreateDatabase hasDynamicNodesDistribution={false} />
            );

            await goNextFromBasicInfoStep(screen, fillInput, fireClick);

            const dynamicDatabaseDistributionSwitch = screen.getByLabelText(/Allow dynamic database distribution/);
            expect(dynamicDatabaseDistributionSwitch).toBeDisabled();

            await user.hover(dynamicDatabaseDistributionSwitch);
            expect(await screen.findByText(/Current license doesn't include/i)).toBeInTheDocument();
        });

        it("can override max replication factor for sharding when is set in license", async () => {
            const { screen, user, fillInput, fireClick } = rtlRender(
                <DefaultCreateDatabase maxReplicationFactorForSharding={2} />
            );

            await goNextFromBasicInfoStep(screen, fillInput, fireClick);

            const replicationFactorInputBeforeShardingEnabled = screen.getAllByName(
                "replicationAndShardingStep.replicationFactor"
            )[0];
            expect(replicationFactorInputBeforeShardingEnabled).toHaveValue(3);

            const shardingSwitch = screen.getByLabelText(/Enable/);
            await user.click(shardingSwitch);

            const replicationFactorInputAfterShardingEnabled = screen.getAllByName(
                "replicationAndShardingStep.replicationFactor"
            )[0];
            expect(replicationFactorInputAfterShardingEnabled).toHaveValue(2);
        });
    });

    describe("From backup", () => {
        it("can disable encryption when the license does not allow it", async () => {
            const { screen, user, fillInput, fireClick } = rtlRender(<DefaultCreateDatabase hasEncryption={false} />);

            await fireClick(screen.getByRole("button", { name: /Restore from backup/ }));
            await goNextFromBasicInfoStep(screen, fillInput, fireClick);

            const encryptionSwitch = screen.getByLabelText(/Encrypt at Rest/);
            expect(encryptionSwitch).toBeDisabled();

            await user.hover(encryptionSwitch);
            expect(await screen.findByText(/Current license doesn't include/i)).toBeInTheDocument();
        });

        it("can disable encryption when server is not secure", async () => {
            const { screen, user, fillInput, fireClick } = rtlRender(<DefaultCreateDatabase isSecureServer={false} />);

            await fireClick(screen.getByRole("button", { name: /Restore from backup/ }));
            await goNextFromBasicInfoStep(screen, fillInput, fireClick);

            const encryptionSwitch = screen.getByLabelText(/Encrypt at Rest/);
            expect(encryptionSwitch).toBeDisabled();
            await user.hover(encryptionSwitch);
            expect(await screen.findByText(/Authentication is off/i)).toBeInTheDocument();
        });
    });

    const goNextFromBasicInfoStep = async (
        screen: RtlScreen,
        fillInput: (element: HTMLElement, value: string) => Promise<void>,
        fireClick: (element: HTMLElement) => Promise<void>
    ) => {
        await fillInput(screen.getByPlaceholderText(/Database name/i), "some-db-name");
        await fireClick(screen.getByText(/Next/));
    };
});
