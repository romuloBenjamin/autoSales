//Modules
const chokidar = require('chokidar');
const fs = require("fs");
const zip = require("node-zip");
const { executeQuery } = require("./database2.js");
//Create a log of what's been done till far
const logs = fs.createWriteStream("./create-files.txt", {flags: 'a'})
const logsql = fs.createWriteStream("./sql-follow-up.txt", {flags: 'a'})
//Date data
let date = new Date()
//let locations = `\\\\172.16.0.19\\Backup-Canhotos\\${date.getFullYear()}\\${date.getMonth()}\\${date.getDate().toString().toString().padStart(2, '0')}`
let locations = { "intra": `\\\\172.16.0.20\\repository\\scanner`, "backup": `\\\\172.16.0.19\\Backup-Canhotos`, "main": "\\\\172.16.0.5\\Organizado" }
//Get data Year, Month & Day -> auto
let year = date.getFullYear();
let month = (date.getMonth() + 1).toString().padStart(2, '0');
let days = (date.getDate() - 1).toString().toString().padStart(2, '0')
//Get data Year, Month & Day -> Manually
//let year = '2022';
//let month = '12';
//Set obj Watcher data
let obj = {};
obj.listar = []
/*----------------------------------------------------------- [Comandos para Logs] -----------------------------------------------------------*/
//Get data Atual
const get_data_logs = async () => {
    //Set Date to Update or Insert dados in SQL
    let date = new Date();
    let data_date = `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`
    let data_hour = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
    return {date: data_date, hour: data_hour}
}
/*-------------------------------------------------------------- [Comandos mv] --------------------------------------------------------------*/
//Create Folders path in Servers if not exists
const create_folder_in_servers = async (path, folder, day) => {
    //Set Watcher Config
    let locationsMain = `${path.main}\\${folder}\\${day}`
    let locationsBks = `${path.backup}\\${folder}\\${day}`
    //Create File if not exist in 19
    if (!fs.existsSync(locationsBks)) fs.mkdirSync(locationsBks, 0777, true);
    //Create File if not exist in 5
    if (!fs.existsSync(locationsMain)) fs.mkdirSync(locationsMain, 0777, true);
}
//Move File to Servers Main & Backup
const move_file_to_server_folder = async (path = null) => {
    //Set New Date
    let date = await get_data_logs()
    //Place dados in Folder & logs
    if (path != null && path != undefined && path != ' ') {
        //Set Splitter paths to create Name with & without file extensions
        let splitter = path.split("\\")
        let fileName_wext = splitter[splitter.length - 1]
        let fileName_woext = splitter[splitter.length - 1].split(".")[0]
        //Save Zip file in Servers
        if (!fs.existsSync(`${locations.main}\\${year}\\${month}\\${days}\\${fileName_woext}.zip`)) {
            let zip2 = new zip();
            //Create data from .PDF to salve in File
            zip2.file(`${fileName_wext}`, fs.readFileSync(path))
            let data = zip2.generate({base64:false, compression:'DEFLATE'})
            //Create File PDF to zip to 19
            fs.writeFileSync(`${locations.backup}\\${year}\\${month}\\${days}\\${fileName_woext}.zip`, data, 'binary')
            //Create File PDF to zip to 5
            fs.writeFileSync(`${locations.main}\\${year}\\${month}\\${days}\\${fileName_woext}.zip`, data, 'binary')
            //set Logs data
            logs.write(`[${date.date} ${date.hour}] Arquivo Criado: ${fileName_woext}.zip\n`)

        } else {
            //set Logs data
            logs.write(`[${date.date} ${date.hour}] Arquivo já Existente: ${fileName_woext}.zip\n`)
        }
    }
}
/*------------------------------------------------------------ [Comandos Mysql] ------------------------------------------------------------*/
//Prepare Mysql to Save in DB
const prepare_save_db = async (path = null) => {
    //Obj to fill info 'cnpj', 'NFe' & 'can_id'
    let dados = {}
    if (path != null && path != undefined && path != ' ') {
        //Get dados from Path and Fill obj dados with CNPJ & NFe
        let splitter = path.split("\\")
        let splitter_wext = splitter[splitter.length - 1]
        let splitter_woext = splitter[splitter.length - 1].split(".")[0]
        //Obj Dados fill Cnpj & NFe
        dados.cnpj = splitter_woext.substr(0, 14);
        dados.nfe = parseInt(splitter_woext.substr(14, splitter_woext.trim().length));
        dados.splitter_wext = splitter_wext; dados.splitter_woext = splitter_woext;
        //
        await save_data_to_db(dados)
    }
}
//Save data to DB
const save_data_to_db = async (dados) => {
    //SQL get 'nf_id' to update Table Canhotos
    let dados_from_nfe_by_cnpj_e_nfe = 'SELECT `nf_id` FROM `empresa_notas` INNER JOIN `empresa_notas_emitentes`'
    dados_from_nfe_by_cnpj_e_nfe += ' ON `empresa_notas`.`nf_emitente` = `empresa_notas_emitentes`.emi_id'
    dados_from_nfe_by_cnpj_e_nfe += ' WHERE `empresa_notas`.`nf_numero` = ? AND `empresa_notas_emitentes`.emi_cnpj = ?'
    //Get Rows & Fields from SQL
    await executeQuery(dados_from_nfe_by_cnpj_e_nfe, [dados.nfe, dados.cnpj]).then(async ([rows, fields]) => {
        //Get NF id to insert or Update Canhotos
        for (const data in rows) {
            if (Object.hasOwnProperty.call(rows, data)) {
                const ids = rows[data];
                await whats_to_do_canhotos(ids.nf_id, dados)
            }
        }
    }).catch(error => console.log(error))
}
//What's to do Insert or Update Canhotos
const whats_to_do_canhotos = async (id, dados) => {
    //Set Date to Update or Insert dados in SQL
    let date = await get_data_logs()
    //Path to Canhotos to Download
    let to_do_canhotos = {status: 0, sql: "", values: []}
    let set_path = `${year}/${month}/${days}/${dados.splitter_woext}.zip`
    //Check If exist Canhoto Registrado
    let check_canhoto = 'SELECT `can_id` FROM `empresa_notas_canhotos` WHERE `can_nota` = ?'
    await executeQuery(check_canhoto, [id]).then(async ([rows, fields]) => {
        //Create SQL to Process
        if (rows.length === 0) {
            to_do_canhotos.status = 1
            to_do_canhotos.sql = `INSERT INTO empresa_notas_canhotos(can_data_cadastro, can_nota, can_url) VALUES(?, ?, ?)`
            to_do_canhotos.values = [`${date.date} ${date.hour}`, `${id}`, `${set_path}`]            
        }
        if (rows.length === 1) {
            to_do_canhotos.status = 2
            to_do_canhotos.sql = `UPDATE empresa_notas_canhotos SET can_url = ? WHERE can_id = ?`
            to_do_canhotos.values = [`${set_path}`, `${rows[0].can_id}`]
        }
        if (rows.length > 1) {
            to_do_canhotos.status = 0
            to_do_canhotos.sql = `VERIFICAR Arquivo em: ${set_path}`
            to_do_canhotos.values = ""
        }
        //Set to do Canhotos
        if ([1,2].includes(to_do_canhotos.status)) {
            await executeQuery(to_do_canhotos.sql, to_do_canhotos.values)
            logsql.write(`[${date.date} ${date.hour}] Registro id: ${id}, em status: ${to_do_canhotos.status}\n`)
        } else logsql.write(`[${date.date} ${date.hour}] Erro ao registrar NFe id: ${id}\n`)
    }).catch(error => console.log(error))
}
/*-------------------------------------------------------- [Comandos Chokidar & Obj] --------------------------------------------------------*/
//Schokidar config
const read_configs = async (path = locations, folder = year + '\\' + month, day = days) => {    
    //Set Watcher Config
    let locationsIntra = `${path.intra}\\${folder}\\${day}`
    let locationsMain = `${path.main}\\${folder}\\${day}`
    let locationsBks = `${path.backup}\\${folder}\\${day}`
    //Set Bks files to 19
    await create_folder_in_servers(path, folder, day)
    //Set Chokidar to Read Intra Files
    obj.watcher = chokidar.watch(`${locationsIntra}`, {
        ignored: /(^|[\/\\])\../,
        awaitWriteFinish: {
            stabilityThreshold: 2000,
            pollInterval: 100
        }
    })
    //Set Watcher URL
    obj.urls = {intra: locationsIntra, main: locationsMain, backup: locationsBks}
}
//Save Files to obj Listar
const save_path_to_obj_listar = async (path) => obj.listar.push(path);
//Create List to upload files
const gerar_listagem_upload = async () => {
    //Watcher on Add event
    obj.watcher.on('add', async (path) => { 
        let data = await get_data_logs()
        await save_path_to_obj_listar(path)
        logs.write(`[${data.date} ${data.hour}] Arquivo Adicionado a Listagem! | Arquivo: ${path}\n`);
    })
    //Get dados when Ready to process    
    obj.watcher.on('ready', () => {
        for (const listar in obj.listar) {
            if (Object.hasOwnProperty.call(obj.listar, listar)) {
                const files = obj.listar[listar];
                //Move File to Servers Main & Backup
                move_file_to_server_folder(files)
                //Prepare to Save in DB
                prepare_save_db(files)
            }
        }
        console.log("finalizado");
    })
    //Get Error in Chokidar Procedurer
    obj.watcher.on('error', error => {})
}
//Schokidar Init
const initialize_single_days = async () => {
    let data = await get_data_logs()
    try {
        logs.write(`-------------------------------------------- [${data.date} ${data.hour}] Iniciando --------------------------------------------\n`)
        logsql.write(`-------------------------------------------- [${data.date} ${data.hour}] Iniciando --------------------------------------------\n`)
        //Create Folder in 5 & 19 (Servers to Bks) & Watcher Config files
        await read_configs()
        //Create List to upload files   
        await gerar_listagem_upload()
    } catch (error) {
        console.log("ola novamente: " . error);
    }
}
initialize_single_days()