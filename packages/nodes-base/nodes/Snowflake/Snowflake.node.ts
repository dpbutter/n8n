import type {
	IExecuteFunctions,
	IDataObject,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeConnectionTypes } from 'n8n-workflow';
import snowflake from 'snowflake-sdk';
import { Readable } from 'stream';

import { getResolvables } from '@utils/utilities';


import {
	connect,
	destroy,
	execute,
	executeStream,
	getConnectionOptions,
	type SnowflakeCredential,
} from './GenericFunctions';


export class Snowflake implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Snowflake',
		name: 'snowflake',
		icon: 'file:snowflake.svg',
		group: ['input'],
		version: 1,
		description: 'Get, add and update data in Snowflake',
		defaults: {
			name: 'Snowflake',
		},
		usableAsTool: true,
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		parameterPane: 'wide',
		credentials: [
			{
				name: 'snowflakeOAuth2Api',
				required: false,
				displayOptions: { show: { authType: ['snowflakeOAuth2Api'] } },
			},
			{
				name: 'snowflake',
				required: true,
				displayOptions: { show: { authType: ['snowflake'] } },
			}
		],
		properties: [
			{
				displayName: 'Authentication Method',
				name: 'authType',
				type: 'options',
				options: [
					{ name: 'OAuth2', value: 'snowflakeOAuth2Api' },
					{ name: 'Key Pair', value: 'snowflake' },
				],
				default: 'snowflakeOAuth2Api',
			},

			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Execute Query',
						value: 'executeQuery',
						description: 'Execute an SQL query',
						action: 'Execute a SQL query',
					},
					{
						name: 'Insert',
						value: 'insert',
						description: 'Insert rows in database',
						action: 'Insert rows in database',
					},
					{
						name: 'Update',
						value: 'update',
						description: 'Update rows in database',
						action: 'Update rows in database',
					},
				],
				default: 'insert',
			},

			// ----------------------------------
			//         executeQuery
			// ----------------------------------
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				noDataExpression: true,
				typeOptions: {
					editor: 'sqlEditor',
				},
				displayOptions: {
					show: {
						operation: ['executeQuery'],
					},
				},
				default: '',
				placeholder: 'SELECT id, name FROM product WHERE id < 40',
				required: true,
				description: 'The SQL query to execute',
			},
			{
				displayName: 'Output Format',
				name: 'outputFormat',
				type: 'options',
				displayOptions: {
					show: {
						operation: ['executeQuery'],
					},
				},
				options: [
					{
						name: 'JSON (Items)',
						value: 'json',
						description: 'Return results as JSON items',
					},
					{
						name: 'CSV File',
						value: 'csv',
						description: 'Stream results to CSV file',
					},
				],
				default: 'json',
				description: 'Format for query results',
			},

			// ----------------------------------
			//         insert
			// ----------------------------------
			{
				displayName: 'Table',
				name: 'table',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['insert'],
					},
				},
				default: '',
				required: true,
				description: 'Name of the table in which to insert data to',
			},
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['insert'],
					},
				},
				default: '',
				placeholder: 'id,name,description',
				description:
					'Comma-separated list of the properties which should used as columns for the new rows',
			},

			// ----------------------------------
			//         update
			// ----------------------------------
			{
				displayName: 'Table',
				name: 'table',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['update'],
					},
				},
				default: '',
				required: true,
				description: 'Name of the table in which to update data in',
			},
			{
				displayName: 'Update Key',
				name: 'updateKey',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['update'],
					},
				},
				default: 'id',
				required: true,
				// eslint-disable-next-line n8n-nodes-base/node-param-description-miscased-id
				description:
					'Name of the property which decides which rows in the database should be updated. Normally that would be "id".',
			},
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['update'],
					},
				},
				default: '',
				placeholder: 'name,description',
				description:
					'Comma-separated list of the properties which should used as columns for rows to update',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const authType = this.getNodeParameter('authType', 0) as string;
		const credentials = await this.getCredentials<SnowflakeCredential>(authType);

		/*
		// Debug: Let's see what we actually got
		console.log('=== CREDENTIAL DEBUG ===');
		console.log('Available credential properties:', Object.keys(credentials));
		console.log('Full credential object:', JSON.stringify(credentials, null, 2));
		console.log('Account field:', credentials.account);
		console.log('========================');
		*/

		let connectionOptions = getConnectionOptions(credentials);
		const connection = snowflake.createConnection(connectionOptions);

		await connect(connection);

		const returnData: INodeExecutionData[] = [];
		const items = this.getInputData();
		const operation = this.getNodeParameter('operation', 0);

		if (operation === 'executeQuery') {
			// ----------------------------------
			//         executeQuery
			// ----------------------------------

			for (let i = 0; i < items.length; i++) {
				let query = this.getNodeParameter('query', i) as string;
				const outputFormat = this.getNodeParameter('outputFormat', i, 'json') as string;

				for (const resolvable of getResolvables(query)) {
					query = query.replace(resolvable, this.evaluateExpression(resolvable, i) as string);
				}

				if (outputFormat === 'csv') {
					// Stream results to CSV using Readable stream (memory-efficient)
					let headers: string[] | null = null;
					let rowCount = 0;
					let streamError: Error | null = null;

					// Create a readable stream that will be populated as rows arrive
					const csvStream = new Readable({
						read() {
							// No-op: data will be pushed from the executeStream callback
						},
					});

					// Start streaming query results (don't await - let prepareBinaryData consume)
					const streamPromise = executeStream(connection, query, [], (row) => {
						if (!headers) {
							// First row - extract headers
							headers = Object.keys(row);
							const headerLine = headers.map(h => `"${h}"`).join(',') + '\n';
							csvStream.push(headerLine);
						}
						// Add data row
						const values = headers!.map(h => {
							const val = row[h];
							if (val === null || val === undefined) return '';
							if (typeof val === 'string') return `"${val.replace(/"/g, '""')}"`;
							return String(val);
						});
						csvStream.push(values.join(',') + '\n');
						rowCount++;
					}).then(() => {
						// Signal end of stream when all rows are processed
						csvStream.push(null);
					}).catch((error) => {
						streamError = error;
						csvStream.destroy(error);
					});

					// Prepare binary data from the stream (n8n will handle storage efficiently)
					// This will consume the stream as data becomes available
					const binaryData = await this.helpers.prepareBinaryData(
						csvStream,
						'query_results.csv',
						'text/csv',
					);

					// Ensure streaming completed without error
					await streamPromise;
					if (streamError) {
						throw streamError;
					}

					returnData.push({
						json: { rowCount, message: `Exported ${rowCount} rows to CSV` },
						binary: { data: binaryData },
						pairedItem: { item: i },
					});
				} else {
					// JSON output (original behavior)
					const responseData = await execute(connection, query, []);
					const executionData = this.helpers.constructExecutionMetaData(
						this.helpers.returnJsonArray(responseData as IDataObject[]),
						{ itemData: { item: i } },
					);
					// Append items individually to avoid memory issues with large result sets
					for (const item of executionData) {
						returnData.push(item);
					}
				}
			}
		}

		if (operation === 'insert') {
			// ----------------------------------
			//         insert
			// ----------------------------------

			const table = this.getNodeParameter('table', 0) as string;
			const columnString = this.getNodeParameter('columns', 0) as string;
			const columns = columnString.split(',').map((column) => column.trim());
			const query = `INSERT INTO ${table}(${columns.join(',')}) VALUES (${columns
				.map((_column) => '?')
				.join(',')})`;
			const data = this.helpers.copyInputItems(items, columns);
			const binds = data.map((element) => Object.values(element));
			await execute(connection, query, binds as unknown as snowflake.InsertBinds);
			data.forEach((d, i) => {
				const executionData = this.helpers.constructExecutionMetaData(
					this.helpers.returnJsonArray(d),
					{ itemData: { item: i } },
				);
				returnData.push(...executionData);
			});
		}

		if (operation === 'update') {
			// ----------------------------------
			//         update
			// ----------------------------------

			const table = this.getNodeParameter('table', 0) as string;
			const updateKey = this.getNodeParameter('updateKey', 0) as string;
			const columnString = this.getNodeParameter('columns', 0) as string;
			const columns = columnString.split(',').map((column) => column.trim());

			if (!columns.includes(updateKey)) {
				columns.unshift(updateKey);
			}

			const query = `UPDATE ${table} SET ${columns
				.map((column) => `${column} = ?`)
				.join(',')} WHERE ${updateKey} = ?;`;
			const data = this.helpers.copyInputItems(items, columns);
			const binds = data.map((element) => Object.values(element).concat(element[updateKey]));
			for (let i = 0; i < binds.length; i++) {
				await execute(connection, query, binds[i] as unknown as snowflake.InsertBinds);
			}
			data.forEach((d, i) => {
				const executionData = this.helpers.constructExecutionMetaData(
					this.helpers.returnJsonArray(d),
					{ itemData: { item: i } },
				);
				returnData.push(...executionData);
			});
		}

		await destroy(connection);
		return [returnData];
	}
}
