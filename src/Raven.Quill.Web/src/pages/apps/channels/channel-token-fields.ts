import { z } from "zod";

// Token format rules shared by the create forms, which must be given a token, and the edit sheet's
// rotation fields, where blank means "leave the stored token alone".

type TokenFormat = {
    isValid: (token: string) => boolean;
    message: string;
};

export const SLACK_BOT_TOKEN_FORMAT: TokenFormat = {
    isValid: (token) => token.startsWith("xoxb-"),
    message: "The bot token starts with xoxb- (not a user or app-level token)",
};

export const DISCORD_BOT_TOKEN_FORMAT: TokenFormat = {
    isValid: (token) => /^\S+$/.test(token),
    message: "A bot token contains no spaces",
};

export function newTokenField(format: TokenFormat, missingMessage: string) {
    return z.string().trim().min(1, missingMessage).refine(format.isValid, format.message);
}

export function rotatedTokenField(format: TokenFormat) {
    return z
        .string()
        .trim()
        .refine((token) => token.length === 0 || format.isValid(token), format.message);
}
