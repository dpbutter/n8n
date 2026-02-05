import { createPrivateKey } from 'crypto';
import pick from 'lodash/pick';
import type snowflake from 'snowflake-sdk';

import { formatPrivateKey } from '@utils/utilities';

import type { IDataObject } from 'n8n-workflow';

const commonConnectionFields = [
	'account',
	'database',
	'schema',
	'warehouse',
	'role',
	'clientSessionKeepAlive',
] as const;

export type SnowflakeCredential = Pick<
	snowflake.ConnectionOptions,
	(typeof commonConnectionFields)[number]
> &
	(
		// Built-in snowflake credential options
		| {
				authentication: 'password';
				username?: string;
				password?: string;
		  }
		| {
				authentication: 'keyPair';
				username: string;
				privateKey: string;
				passphrase?: string;
		  }
		| { // Snowflake OAuth2 credential
				grantType: string;
				authUrl: string;
				accessTokenUrl: string;
				clientId: string;
				clientSecret: string;
				scope: string;
				authQueryParameters: string;
				authentication: 'Header' | 'Body';
				oauthTokenData: IDataObject;
			}
	);

const extractPrivateKey = (credential: { privateKey: string; passphrase?: string }) => {
	const key = formatPrivateKey(credential.privateKey);

	if (!credential.passphrase) return key;

	const privateKeyObject = createPrivateKey({
		key,
		format: 'pem',
		passphrase: credential.passphrase,
	});

	return privateKeyObject.export({
		format: 'pem',
		type: 'pkcs8',
	}) as string;
};

export const getConnectionOptions = (credential: SnowflakeCredential) => {
	// Grab the common connection field values
	const connectionOptions: snowflake.ConnectionOptions = pick(credential, commonConnectionFields);

	if ('oauthTokenData' in credential) {
		// Snowflake OAuth2 credential
		connectionOptions.authenticator =  'OAUTH';
		connectionOptions.token = credential.oauthTokenData.access_token as string;
	} else {
		// Standard Snowflake credential
		if (credential.authentication === 'keyPair') {
			connectionOptions.authenticator = 'SNOWFLAKE_JWT';
			connectionOptions.username = credential.username;
			connectionOptions.privateKey = extractPrivateKey(credential);
		} else { // credential.authentication === 'password'
			connectionOptions.username = credential.username;
			connectionOptions.password = credential.password;
		}
	}
	return connectionOptions;
};

export async function connect(conn: snowflake.Connection) {
	return await new Promise<void>((resolve, reject) => {
		conn.connect((error) => (error ? reject(error) : resolve()));
	});
}

export async function destroy(conn: snowflake.Connection) {
	return await new Promise<void>((resolve, reject) => {
		conn.destroy((error) => (error ? reject(error) : resolve()));
	});
}

export async function execute(
	conn: snowflake.Connection,
	sqlText: string,
	binds: snowflake.InsertBinds,
) {
	return await new Promise<any[] | undefined>((resolve, reject) => {
		conn.execute({
			sqlText,
			binds,
			complete: (error, _, rows) => (error ? reject(error) : resolve(rows)),
		});
	});
}

export async function executeStream(
	conn: snowflake.Connection,
	sqlText: string,
	binds: snowflake.InsertBinds,
	onRow: (row: any) => void,
) {
	return await new Promise<void>((resolve, reject) => {
		conn.execute({
			sqlText,
			binds,
			streamResult: true,
			complete: (error, stmt) => {
				if (error) {
					reject(error);
				} else {
					// Don't resolve here - let the stream end event handle it
					stmt.streamRows()
						.on('data', onRow)
						.on('error', reject)
						.on('end', resolve);
				}
			},
		});
	});
}
