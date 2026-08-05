import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { channelsMocks, SAMPLE_WHATSAPP_CHANNEL_ID } from "@/mocks/channels-mocks";
import { embedLinksMocks } from "@/mocks/embed-links-mocks";
import { AppChannels } from "./app-channels";

const meta = {
    title: "Apps/Channels",
    component: AppChannels,
    parameters: {
        page: { title: "Channels" },
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppChannels>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: {
            handlers: {
                channels: [channelsMocks.list([])],
            },
        },
    },
};

// The whole add-channel happy path for WhatsApp Personal: pick the menu entry,
// create the channel, and land on the in-sheet QR pairing phase.
export const CreateWhatsAppPersonal: Story = {
    parameters: {
        msw: {
            handlers: {
                channels: [
                    channelsMocks.list(),
                    channelsMocks.create({ channelId: SAMPLE_WHATSAPP_CHANNEL_ID }),
                    channelsMocks.update(),
                    channelsMocks.delete(),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // the dropdown menu and sheet render in portals outside the canvas element
        const body = within(canvasElement.ownerDocument.body);

        await userEvent.click(await canvas.findByRole("button", { name: /new channel/i }));
        await userEvent.click(await body.findByRole("menuitem", { name: /whatsapp personal/i }));

        const sheet = within(await body.findByRole("dialog"));
        await userEvent.click(sheet.getByRole("combobox", { name: /agent/i }));
        await userEvent.click(await body.findByRole("option", { name: /faq bot/i }));
        await userEvent.click(sheet.getByRole("button", { name: /create channel/i }));

        await waitFor(() =>
            expect(sheet.getByRole("img", { name: /whatsapp pairing qr code/i })).toBeInTheDocument(),
        );
    },
};

// Open "Generate embed link" on the Website widget card and submit to see the
// server's inline error surfaced (e.g. a required parameter left blank server-side).
export const LinkMintError: Story = {
    parameters: {
        msw: {
            handlers: {
                // Overriding a service replaces its whole handler array, so keep the list
                // endpoint and only swap the mint one for the failing variant.
                embedLinks: [embedLinksMocks.list(), embedLinksMocks.mintError()],
            },
        },
    },
};
