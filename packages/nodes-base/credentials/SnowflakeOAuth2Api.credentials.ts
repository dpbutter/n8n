import type {
	Icon,
	ICredentialType,
	INodeProperties
} from 'n8n-workflow';


export class SnowflakeOAuth2Api implements ICredentialType {
	name = 'snowflakeOAuth2Api';
	extends = ['oAuth2Api'];

	displayName = 'Snowflake OAuth2 API';

	documentationUrl = 'https://docs.snowflake.com/en/user-guide/oauth';

	properties: INodeProperties[] = [

		/****************************************/
		/* Generic OAuth2 fields with overrides */
		/****************************************/

		{
			displayName: 'Grant Type',
			name: 'grantType',
			type: 'options',
			options: [
				{
					name: 'Authorization Code',
					value: 'authorizationCode',
				},
				{
					name: 'Client Credentials',
					value: 'clientCredentials',
				},
				{
					name: 'PKCE',
					value: 'pkce',
				},
			],
			default: 'pkce',
			description: 'The OAuth2 grant type to use for authentication',
		},
		// Modify OAuth2 fields to give default values
		{
      displayName: 'Authorization URL',
      name: 'authUrl',
      type: 'string',
      default: 'https://ACCOUNT.snowflakecomputing.com/oauth/authorize',
    },
    {
      displayName: 'Access Token URL',
      name: 'accessTokenUrl',
      type: 'string',
      default: 'https://ACCOUNT.snowflakecomputing.com/oauth/token-request',
    },
		{
			displayName: 'Scope',
			name: 'scope',
			type: 'string',
			default: '',
		},


		/*****************************/
		/* Standard Snowflake fields */
		/*****************************/
		{
			displayName: 'Account',
			name: 'account',
			type: 'string',
			default: '',
			required: true,
			description: 'Enter the name of your Snowflake account',
		},
		{
			displayName: 'Database',
			name: 'database',
			type: 'string',
			default: '',
			description: 'Specify the database you want to use after creating the connection',
		},
		{
			displayName: 'Warehouse',
			name: 'warehouse',
			type: 'string',
			default: '',
			description:
				'The default virtual warehouse to use for the session after connecting. Used for performing queries, loading data, etc.',
		},
		{
			displayName: 'Schema',
			name: 'schema',
			type: 'string',
			default: '',
			description: 'Enter the schema you want to use after creating the connection',
		},
		{
			displayName: 'Role',
			name: 'role',
			type: 'string',
			default: '',
			description: 'Enter the security role you want to use after creating the connection',
		},
		{
			displayName: 'Client Session Keep Alive',
			name: 'clientSessionKeepAlive',
			type: 'boolean',
			default: false,
			description:
				'Whether to keep alive the client session. By default, client connections typically time out approximately 3-4 hours after the most recent query was executed. If the parameter clientSessionKeepAlive is set to true, the client’s connection to the server will be kept alive indefinitely, even if no queries are executed.',
		},
	];
}
