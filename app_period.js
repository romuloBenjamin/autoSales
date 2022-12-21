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
//Get data Year, Month & Day
let pediod = {start: "2022-01-01", end: "2022-02-28"}
//Set obj Watcher data
let obj = {};
obj.listar = {}
obj.newListar = {}
/*----------------------------------------------------------- [Comandos para Logs] -----------------------------------------------------------*/
//Get data Atual
const get_data_logs = async (data) => {
    //Set Date to Update or Insert dados in SQL
    let date = new Date(data);
    let data_date = `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate().toString().padStart(2, '0')}`
    let data_hour = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
    return {date: data_date, hour: data_hour, year: `${date.getFullYear()}`, month: `${(date.getMonth() + 1).toString().padStart(2, '0')}`, days: `${date.getDate().toString().padStart(2, '0')}`}
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
//Create Loop to Move to Folder Main & Backup
const loop_move_files_to_server = async (property, rows, days) => {
    for (let index = 0; index < rows.length; index++) {
        const files = rows[index];
        await move_file_to_server_folder(files, days)
    }
}
//Move File to Servers Main & Backup
const move_file_to_server_folder = async (path = null, days) => {
    //Set New Date
    let date = await get_data_logs(days)
    //Set Splitter paths to create Name with & without file extensions    
    let splitter = path.split("\\")
    let file_paths = `${splitter[splitter.length - 4]}\\${splitter[splitter.length - 3]}\\${splitter[splitter.length - 2]}`
    //Set Splitter Filenames With & Without Extensions
    let fileName_wext = splitter[splitter.length - 1]
    let fileName_woext = splitter[splitter.length - 1].split(".")[0]
    //Save Zip file in Servers
    if (!fs.existsSync(`${locations.main}\\${file_paths}\\${fileName_woext}.zip`)) {
        let zip2 = new zip();
        //Create data from .PDF to salve in File
        zip2.file(`${fileName_wext}`, fs.readFileSync(path))
        let data = zip2.generate({base64:false, compression:'DEFLATE'})
        //Create File PDF to zip to 19
        fs.writeFileSync(`${locations.backup}\\${file_paths}\\${fileName_woext}.zip`, data, 'binary')
        //Create File PDF to zip to 5
        fs.writeFileSync(`${locations.main}\\${file_paths}\\${fileName_woext}.zip`, data, 'binary')
        //set Logs data
        logs.write(`[${date.date} ${date.hour}] Ano e Mes: ${splitter[splitter.length - 4]}/${splitter[splitter.length - 3]}, dia: ${splitter[splitter.length - 2]}, Arquivo Criado: ${fileName_woext}.zip\n`)
    }
    else {
        //set Logs data
        logs.write(`[${date.date} ${date.hour}] Ano e Mes: ${splitter[splitter.length - 4]}/${splitter[splitter.length - 3]}, dia: ${splitter[splitter.length - 2]}, Arquivo já Existente: ${fileName_woext}.zip\n`)
    }
}
/*------------------------------------------------------------ [Comandos Mysql] ------------------------------------------------------------*/
//Prepare Mysql to Save in DB
const prepare_save_db = async (sets, path = null, days) => {
    //Obj to fill info 'cnpj', 'NFe' & 'can_id'
    for (let index = 0; index < path.length; index++) {
        const files = path[index];
        //Create Variable to this Process
        let dados = {}
        //Get dados from Path and Fill obj dados with CNPJ & NFe
        let splitter = files.split("\\")
        let splitter_wext = splitter[splitter.length - 1]
        let splitter_woext = splitter[splitter.length - 1].split(".")[0]
        //Obj Dados fill Cnpj & NFe
        dados.cnpj = splitter_woext.substr(0, 14);
        dados.nfe = parseInt(splitter_woext.substr(14, splitter_woext.trim().length));
        dados.splitter_woext = splitter_woext; dados.splitter_wext = splitter_wext; dados.data = days;
        //Save DB data        
        await save_data_to_db(dados, sets)
    }    
}
//Save data to DB
const save_data_to_db = async (dados, sets) => {
    //SQL get 'nf_id' to update Table Canhotos
    let dados_from_nfe_by_cnpj_e_nfe = 'SELECT `empresa_notas`.`nf_id` FROM `empresa_notas`'
    dados_from_nfe_by_cnpj_e_nfe += ' INNER JOIN `empresa_notas_emitentes`'
    dados_from_nfe_by_cnpj_e_nfe += ' ON `empresa_notas`.`nf_emitente` = `empresa_notas_emitentes`.`emi_id`'
    dados_from_nfe_by_cnpj_e_nfe += ' WHERE `empresa_notas`.`nf_numero` = ? AND `empresa_notas_emitentes`.`emi_cnpj` = ?'
    //Get Rows & Fields from SQL
    await executeQuery(dados_from_nfe_by_cnpj_e_nfe, [`${dados.nfe}`, dados.cnpj]).then(async (rows, fields) => {
        Object.values(rows[0]).forEach(async (nfeid) => {
            let conf = "SELECT `can_id`, `can_url` FROM `empresa_notas_canhotos` WHERE `can_nota` = '?'"
            await executeQuery(conf, [nfeid.nf_id]).then(async (rows2, fields) => {
                //Set Date to Update or Insert dados in SQL
                console.log(rows2[0]);
                if (rows2[0].length === 0) {
                    let data = new Date()
                    let current_date = `${data.getFullYear()}-${data.getMonth() + 1}-${data.getDate()} ${data.getHours()}:${data.getMinutes()}:${data.getSeconds()}`
                    let insert = "INSERT INTO empresa_notas_canhotos(can_data_cadastro, can_nota, can_url) VALUES(?, ?, ?)"
                    logsql.write(`Canhoto Cadastrado referencia: ${nfeid.nf_id}, cadastrada: ${current_date}, path: ${sets}/${dados.splitter_woext}.zip\n`)
                    await executeQuery(insert, [current_date, `${nfeid.nf_id}`, `${sets}/${dados.splitter_woext}.zip`])
                } else {
                    if (nfeid.can_url !== `${sets}/${dados.splitter_woext}.zip`) {
                        let update = "UPDATE empresa_notas_canhotos SET can_url = ? WHERE can_nota = ?"
                        logsql.write(`Canhoto Já cadastrado Nfe Referencia: ${nfeid.nf_id}, updated path: ${sets}/${dados.splitter_woext}.zip\n`)
                        await executeQuery(update, [`${sets}/${dados.splitter_woext}.zip`, `${nfeid.nf_id}`])
                    } else { console.log("URL Atual cadastrada!"); }
                }
            }).catch((error) => console.log(error))
        })
    })
    .catch((error) => logsql.write(`Nfe não localizada: ${dados.nfe}, cnpj: ${dados.cnpj}\n`));
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
    obj.watcher = chokidar.watch(`${locationsMain}`, {
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
const save_path_to_obj_listar = async (path) => {
    let splitter = path.split("\\")
    let get_this_year = splitter[splitter.length - 4]
    let get_this_month = splitter[splitter.length - 3]
    let get_this_day = splitter[splitter.length - 2]
    //Create Obj listar para Periodo
    if (!obj.listar.hasOwnProperty(`${get_this_year}/${get_this_month}/${get_this_day}`)) obj.listar[`${get_this_year}/${get_this_month}/${get_this_day}`] = []
    obj.listar[`${get_this_year}/${get_this_month}/${get_this_day}`].push(path)
}
//Create List to upload files
const gerar_listagem_upload = async (days) => {
    //Watcher on Add & Change Event
    obj.watcher.on('add', async (path) => await save_path_to_obj_listar(path))
    obj.watcher.on('change', async (path) => await save_path_to_obj_listar(path))
    //Get dados when Ready to process    
    obj.watcher.on('ready', async () => {
        //Move File to Servers Main & Backup
        //Object.entries(obj.listar).forEach(async (rows) => await loop_move_files_to_server(rows[0], rows[1], days));
        //Prepare Update DB
        Object.entries(obj.listar).forEach(async (rows) => await prepare_save_db(rows[0], rows[1], days))
    })
    //Get Error in Chokidar Procedurer
    obj.watcher.on('error', error => {})
}
//Schokidar Init
const initialize_period = async () => {
    let data_start = new Date(`${pediod.start} 00:00:00`)
    let date_end = new Date(`${pediod.end} 00:00:00`)
    //Periodo de datas para atualização
    for (let days = data_start; days <= date_end; days.setDate(days.getDate() + 1)) {
        try {
            const data = await get_data_logs(days)
            //Create Folder in 5 & 19 (Servers to Bks) & Watcher Config files
            await read_configs(locations, `${data.year}\\${data.month}`, `${data.days}`)            
            //Create List to upload files
            await gerar_listagem_upload(days)
        } catch (error) {
            console.log("ola novamente: " . error);
        }
    }
}
initialize_period()