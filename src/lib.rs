use lazy_static::lazy_static;
use log::info;
use smol::channel::{unbounded, Sender};
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaTransactionInfoVersions, Result as GeyserResult, SlotStatus,
};
use solana_sdk::{signature::Signature, transaction::SanitizedTransaction};
use solana_transaction_status::TransactionStatusMeta;

mod account_filter;
pub use account_filter::*;

#[derive(Debug)]
pub enum AccTx {
    Acc {
        pubkey: Vec<u8>,
        lamports: u64,
        owner: Vec<u8>,
        executable: bool,
        rent_epoch: u64,
        data: Vec<u8>,
        write_version: u64,
        txn_signature: Option<Signature>,
        slot: u64,
        is_startup: bool,
    },
    Tx {
        slot: u64,
        signature: Signature,
        is_vote: bool,
        transaction: SanitizedTransaction,
        transaction_status_meta: TransactionStatusMeta,
        index: Option<usize>,
    },
}

impl Default for AccTx {
    fn default() -> Self {
        AccTx::Acc {
            pubkey: Vec::default(),
            lamports: u64::default(),
            owner: Vec::default(),
            executable: bool::default(),
            rent_epoch: u64::default(),
            data: Vec::default(),
            write_version: u64::default(),
            txn_signature: Option::default(),
            slot: u64::default(),
            is_startup: bool::default(),
        }
    }
}

impl AccTx {
    pub fn to_string(&self) -> String {
        format!("{:?}", self)
    }

    pub fn into_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    pub fn into_acc(slot: u64, is_startup: bool, value: &ReplicaAccountInfoVersions) -> Self {
        match value {
            ReplicaAccountInfoVersions::V0_0_1(inner_account) => Self::Acc {
                pubkey: inner_account.pubkey.to_owned(),
                lamports: inner_account.lamports,
                owner: inner_account.owner.to_owned(),
                executable: inner_account.executable,
                rent_epoch: inner_account.rent_epoch,
                data: inner_account.data.to_owned(),
                write_version: inner_account.write_version,
                txn_signature: Option::default(),
                slot,
                is_startup,
            },
            ReplicaAccountInfoVersions::V0_0_2(inner_account) => Self::Acc {
                pubkey: inner_account.pubkey.to_owned(),
                lamports: inner_account.lamports,
                owner: inner_account.owner.to_owned(),
                executable: inner_account.executable,
                rent_epoch: inner_account.rent_epoch,
                data: inner_account.data.to_owned(),
                write_version: inner_account.write_version,
                txn_signature: inner_account.txn_signature.cloned(),
                slot,
                is_startup,
            },
        }
    }

    pub fn into_tx(slot: u64, value: &ReplicaTransactionInfoVersions) -> Self {
        match value {
            ReplicaTransactionInfoVersions::V0_0_1(inner_tx) => Self::Tx {
                slot,
                signature: inner_tx.signature.to_owned(),
                is_vote: inner_tx.is_vote,
                transaction: inner_tx.transaction.to_owned(),
                transaction_status_meta: inner_tx.transaction_status_meta.to_owned(),
                index: Option::default(),
            },

            ReplicaTransactionInfoVersions::V0_0_2(inner_tx) => Self::Tx {
                slot,
                signature: inner_tx.signature.to_owned(),
                is_vote: inner_tx.is_vote,
                transaction: inner_tx.transaction.to_owned(),
                transaction_status_meta: inner_tx.transaction_status_meta.to_owned(),
                index: Some(inner_tx.index),
            },
        }
    }
}

lazy_static! {
    static ref SENDER: Sender<AccTx> = {
        let (sender, receiver) = unbounded::<AccTx>();

        use std::{fs::File, io::prelude::*};

        let mut accs_file = File::create("./accs.txt").unwrap();
        let mut txs_file = File::create("./txs.txt").unwrap();

        smol::spawn(async move {
            while let Ok(value) = receiver.recv().await {
                match value {
                    AccTx::Acc { .. } => {
                        accs_file.write_all(&value.into_bytes()).unwrap();
                    }
                    AccTx::Tx { .. } => {
                        txs_file.write_all(&value.into_bytes()).unwrap();
                    }
                }
            }
        })
        .detach();

        sender
    };
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = FusionEnginePlugin::new();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}

#[derive(Debug)]
pub struct FusionEnginePlugin {}

impl FusionEnginePlugin {
    pub fn new() -> Self {
        FusionEnginePlugin {}
    }
}

impl GeyserPlugin for FusionEnginePlugin {
    fn name(&self) -> &'static str {
        "FusionEnginePlugin"
    }

    fn on_load(&mut self, config_file: &str) -> GeyserResult<()> {
        solana_logger::setup_with_default("info");
        info!(
            "Loading plugin {:?} from config_file {:?}",
            self.name(),
            config_file
        );

        Ok(())
    }

    fn on_unload(&mut self) {}

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> GeyserResult<()> {
        let outcome = AccTx::into_acc(slot, is_startup, &account);

        smol::block_on(async move {
            smol::spawn(async move { SENDER.send(outcome).await })
                .await
                .unwrap();
        });

        Ok(())
    }

    fn notify_transaction(
        &mut self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> GeyserResult<()> {
        let outcome = AccTx::into_tx(slot, &transaction);

        smol::block_on(async move {
            smol::spawn(async move { SENDER.send(outcome).await }).detach();
        });

        Ok(())
    }

    fn notify_block_metadata(&mut self, _blockinfo: ReplicaBlockInfoVersions) -> GeyserResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &mut self,
        _slot: u64,
        _parent: Option<u64>,
        _status: SlotStatus,
    ) -> GeyserResult<()> {
        Ok(())
    }

    fn notify_end_of_startup(&mut self) -> GeyserResult<()> {
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}
