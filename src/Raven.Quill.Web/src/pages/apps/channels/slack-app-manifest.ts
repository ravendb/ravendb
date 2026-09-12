export const SLACK_APP_MANIFEST = `display_information:
  name: Quill
  description: Chat with your data through Quill.
features:
  app_home:
    messages_tab_enabled: true
    messages_tab_read_only_enabled: false
  bot_user:
    display_name: Quill
    always_online: true
oauth_config:
  scopes:
    bot:
      - chat:write
      - im:history
      - users:read
      - users:read.email
settings:
  interactivity:
    is_enabled: false
  org_deploy_enabled: false
  socket_mode_enabled: false
  token_rotation_enabled: false
`;
