//Modules
const chokidar = require('chokidar');
const fs = require("fs");
const parser = require("xml2json");
const ramString = require("randomstring");
const { executeQuery } = require("./database2.js");
const { exec } = require('child_process');
//Create a log of what's been done till far
const logs = fs.createWriteStream("./create-files-nfe.txt", {flags: 'a'})
const logsql = fs.createWriteStream("./sql-follow-up-nfe.txt", {flags: 'a'})
//Get data Year, Month & Day -> auto
let date = new Date()
let year = date.getFullYear();
let month = date.getMonth() + 1;
let days = date.getDate().toString().toString().padStart(2, '0')
//Get data Year, Month & Day -> Manually
//let year = '2022';
//let month = '12';
//let days = '15'
//Set Locations & cnpjs
let locations = {}
locations.path = "\\\\172.16.0.33\\Uni\\Uni40\\UniNFe"
locations.cnpjs = ["10290557000168", "21823607000141", "30379727000192", "35765246000139", "47498059000115", "66826918000100"]
locations.complementos = "Enviados\\Autorizados"
//Set obj Watcher data
let obj = {}
let nfe = {}
obj.listar = {}
/*----------------------------------------------------------- [Comandos para Logs] -----------------------------------------------------------*/
//Get data Atual
const get_data_logs = async () => {
    //Set Date to Update or Insert dados in SQL
    let date = new Date();
    let data_date = `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`
    let data_hour = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
    return {date: data_date, hour: data_hour}
}
/*------------------------------------------------------------- [Comandos XML] -------------------------------------------------------------*/
//Get dados from XML
const open_xml_to_json_data = async (nfePath) => {    
    let read_xml = fs.readFileSync(nfePath)
    let jsonXML = JSON.parse(parser.toJson(read_xml))
    let nfe_data = jsonXML.nfeProc
    let nfe_idata = nfe_data?.NFe.infNFe
    //Date Date Emissão    
    let date = new Date(nfe_idata?.ide?.dhEmi)
    //Set NFe Code
    nfe.code = ramString.generate(10)
    nfe.version = nfe_data?.versao
    nfe.nfe = nfe_idata?.ide?.nNF
    nfe.emissao = `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()} ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`
    nfe.emitente = {}
    nfe.emitente.cnpj = nfe_idata?.emit?.CNPJ
    nfe.emitente.empresa = nfe_idata?.emit?.xNome
    nfe.destinatario = {}
    nfe.destinatario.docs = (nfe_idata?.dest.hasOwnProperty("CNPJ"))? nfe_idata?.dest?.CNPJ: nfe_idata?.dest?.CPF;
    nfe.destinatario.cliente = nfe_idata?.dest?.xNome
    nfe.destinatario.email = (nfe_idata?.dest?.email != undefined)? nfe_idata?.dest?.email: null;
    nfe.key = nfe_data?.protNFe?.infProt?.chNFe
    nfe.valor = nfe_idata?.total?.ICMSTot?.vNF
    //Save SQL Verificar Se Existe Cadastro
    if (nfe.version !== '' && nfe.version !== null, nfe.version !== undefined) await check_if_exist_nfe_keys(nfe)
}
/*------------------------------------------------------------ [Comandos Mysql] ------------------------------------------------------------*/
//Check if Exist NFe Key
const check_if_exist_nfe_keys = async (dados) => {
    await executeQuery(`SELECT nf_id FROM empresa_notas WHERE nf_chave = ?`, [`${dados.key}`]).then(async ([rows, fields]) => {
        if (rows.length === 0) await add_nfe_key(dados)
        else console.log(`Chave Nfe: ${dados.key}, já cadastrada, id do registro: ${rows[0].nf_id}`);
    }).catch((error) => { console.log(error); })
}
//Add Key if not exist
const add_nfe_key = async (dados) => {
    await executeQuery(
        `CALL empresa_notas_iupdate (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
            `${dados.emitente.empresa}`,
            `${dados.emitente.cnpj}`,
            `${dados.destinatario.cliente}`,
            `${dados.destinatario.docs}`,
            `${dados.destinatario.email}`,
            `${dados.emissao}`,
            `${dados.nfe}`,
            `${dados.key}`,
            `${dados.valor}`,
            `${dados.version}`,
            ``,
            `${dados.code}`
        ]
    )
    //Logar Registros
    console.log(`Chave Adicionada Nfe: ${dados.key}`);
}
/*-------------------------------------------------------- [Comandos Chokidar & Obj] --------------------------------------------------------*/
//Schokidar config
const read_configs = async (path = locations, cnpj = "cnpj do cliente") => {    
    //Set Watcher Config
    let locationsIntra = `${path.path}\\${cnpj}\\${path.complementos}\\${year}\\${month}\\${days}`
    //Set Chokidar to Read Intra Files
    obj.watcher = chokidar.watch(`${locationsIntra}`, {
        ignored: /(^|[\/\\])\../,
        awaitWriteFinish: {
            stabilityThreshold: 2000,
            pollInterval: 100
        }
    })
    //Set Watcher URL
    obj.urls = {intra: locationsIntra}
}
//Save Files to obj Listar
const save_path_to_obj_listar = async (path, cnpj) => {
    if (!obj.listar.hasOwnProperty(cnpj)) obj.listar[cnpj] = []
    obj.listar[cnpj].push(path)
}
//Create List to upload files
const gerar_listagem_upload = async (empresa) => {
    //Watcher on Add event
    obj.watcher.on('add', async (path) => await save_path_to_obj_listar(path, empresa))
    obj.watcher.on('change', async (path) => await save_path_to_obj_listar(path, empresa))
    //Get dados when Ready to process    
    obj.watcher.on('ready', async () => {
        for (const listar in obj.listar) {
            if (Object.hasOwnProperty.call(obj.listar, listar)) {
                const files = obj.listar[listar];
                for (const nfe of files) {
                    let splitter = nfe.split("\\")
                    let xml_name = splitter[splitter.length - 1]
                    let procNFE = xml_name.split("-procNFe")
                    //Move File to Servers Main & Backup
                    if (procNFE.length === 2) await open_xml_to_json_data(nfe)
                }
            }
        }
        console.log("finalizado");
    })
    //Get Error in Chokidar Procedurer
    obj.watcher.on('error', error => {})
}
//Schokidar Init
const initialize_single_days_nfe = async () => {
    let data = await get_data_logs()
    try {
        for (let index = 0; index < locations.cnpjs.length; index++) {
            const empresa = locations.cnpjs[index];
            logs.write(`-------------------------------------------- [${data.date} ${data.hour}] Iniciando NFE ${empresa} --------------------------------------------\n`)
            logsql.write(`-------------------------------------------- [${data.date} ${data.hour}] Iniciando NFE ${empresa} --------------------------------------------\n`)
            //Create Folder in 5 & 19 (Servers to Bks) & Watcher Config files
            await read_configs(locations, empresa)
            //Create List to upload files   
            await gerar_listagem_upload(empresa)
        }
    } catch (error) {
        console.log("ola novamente: " . error);
    }
}
initialize_single_days_nfe()