
type Feature = "Sharding" | "Styling" | "Other";

type TeamMember = "Marcin" | "Danielle" | "Kwiato" | "Damian" | "ANY";

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function todo(feature: Feature, member: TeamMember, message = "TODO") {
    // empty
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function shardingTodo(member: TeamMember = "ANY", message = "TODO") {
    // empty
}
