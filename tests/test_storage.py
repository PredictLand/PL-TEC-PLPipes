from pathlib import Path
import tempfile
import shutil

from unittest.mock import Mock, patch, MagicMock

import plpipes.cloud.azure.storage as st



class MockResourceGroupsList:
    def __init__(self, resource_groups):
        self._resource_groups = resource_groups
    
    def by_page(self):
        yield self._resource_groups

def create_mock_directory(name, files=None, directories=None):
    mock_directory = Mock()
    mock_directory.name = name
    if files:
        mock_directory.listdir.return_value = [Mock(name=file) for file in files]
    if directories:
        mock_directory.listdir.return_value = [create_mock_directory(dir) for dir in directories]
    return mock_directory


@patch("plpipes.cloud.azure.storage.SubscriptionClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_show_subscriptions(mock_cred, mock_subscription_client, capsys):

    # MOCKING SETUP
    mock_subscription_client.return_value = Mock()
    mock_subscription_client.return_value.subscriptions.list.return_value = [Mock(subscription_id="59e50b96-14bf-4a96-bcab-32bf697494d7", display_name="PredictLand One")]

    # RUN METHOD TO TEST
    st.show_subscriptions("predictland2")

    # STDOUT ASSERTIONS
    captured = capsys.readouterr()

    assert f"These are the subscriptions available with your user predictland2" in captured.out
    assert f"---------------Subscription 1---------------" in captured.out
    assert "Subscription ID: 59e50b96-14bf-4a96-bcab-32bf697494d7" in captured.out
    assert "Subscription Name: PredictLand One" in captured.out


@patch("plpipes.cloud.azure.storage.ResourceManagementClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_show_resource_groups(mock_cred, mock_resource_management_client, capsys):

    # MOCKING SETUP
    mock_resource_group_1 = MagicMock()
    mock_resource_group_1.name = "plpipes-dev"
    mock_resource_group_2 = MagicMock()
    mock_resource_group_2.name = "plpipes-test"
    mock_resources_groups_list = [mock_resource_group_1, mock_resource_group_2]
    mock_resource_group_list = MockResourceGroupsList(mock_resources_groups_list)
    mock_resource_management_client.return_value.resource_groups.list.return_value = mock_resource_group_list

    # RUN METHOD TO TEST
    st.show_resource_groups("predictland2", "PredictLand One")

    # STDOUT ASSERTIONS
    captured = capsys.readouterr()

    assert f"These are the resources groups available in the subscription PredictLand One" in captured.out
    assert f"---------------Resource group 1---------------" in captured.out
    assert f"Resource group name: plpipes-dev" in captured.out
    assert f"---------------Resource group 2---------------" in captured.out
    assert f"Resource group name: plpipes-test" in captured.out


@patch("plpipes.cloud.azure.storage.StorageManagementClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_show_storage_accounts(mock_cred, mock_storage_management_client, capsys):

    # MOCKING SETUP
    mock_storage_accounts = [MagicMock(name="developing")]
    mock_storage_accounts[0].name = "developing"
    mock_storage_management_client.return_value.storage_accounts.list_by_resource_group.return_value = mock_storage_accounts
    
    # RUN METHOD TO TEST
    st.show_storage_accounts("predictland2", "plpipes-dev", "PredictLand One")

    # STDOUT ASSERTIONS
    captured = capsys.readouterr()

    assert f"These are the storage accounts available with your user predictland2 within the subscription PredictLand One and the resource group plpipes-dev" in captured.out
    assert f"---------------Storage Account 1 from subscription PredictLand One and resource group plpipes-dev---------------" in captured.out
    assert f"Storage Account Name: developing" in captured.out


@patch("plpipes.cloud.azure.storage.BlobServiceClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_containers_dir_ls(mock_cred, mock_blob_service_client, capsys):

    # MOCKING SETUP
    mock_container = MagicMock(name="mock_container")
    mock_container.name = "oscar-container"
    mock_blob_service_client.return_value.list_containers.return_value = [mock_container]

    # RUN METHOD TO TEST
    containers_dir = st._ContainersDir(st._FS("predictland2", "PredictLand One", "developing", "plpipes-dev"), "\containers")
    containers_dir.ls()

    # ASSERTIONS
    captured = capsys.readouterr()

    # check the containers directory has the correct containers
    assert containers_dir._containers == {"oscar-container": st._Container}

    # stdout assertions
    assert "oscar-container" in captured.out


@patch("plpipes.cloud.azure.storage.BlobServiceClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_container_ls(mock_cred, mock_blob_service_client, capsys):

    # MOCKING SETUP
    mock_blob1 = MagicMock(name="mock_blob1")
    mock_blob1.name = "Fichero prueba MSTeams.txt"
    mock_blob2 = MagicMock(name="mock_blob2")
    mock_blob2.name = "foo-dir"
    mock_blob3 = MagicMock(name="mock_blob3")
    mock_blob3.name = "foo-dir/Fichero prueba MSTeams.txt"
    mock_blob4 = MagicMock(name="mock_blob4")
    mock_blob4.name = "foo-dir/First Blob.txt"
    mock_container_client = MagicMock()
    mock_container_client.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3, mock_blob4]
    mock_blob_service_client.return_value.get_container_client.return_value = mock_container_client

    # RUN METHOD TO TEST
    container = st._Container(st._FS("predictland2", "PredictLand One", "developing", "plpipes-dev"), "\containers\oscar-container", "oscar-container")
    container.ls()

    # ASSERTIONS
    captured = capsys.readouterr()

    # check the container has the correct blobs
    assert container._blobs == {"Fichero prueba MSTeams.txt": st._Blob, 
                                "foo-dir": st._Blob,
                                "foo-dir/Fichero prueba MSTeams.txt": st._Blob,
                                "foo-dir/First Blob.txt": st._Blob}
    
    # stdout assertions
    assert "Fichero prueba MSTeams.txt" in captured.out
    assert "foo-dir" in captured.out
    assert "foo-dir/Fichero prueba MSTeams.txt" in captured.out
    assert "foo-dir/First Blob.txt" in captured.out


@patch("plpipes.cloud.azure.storage.BlobServiceClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_blob_get(mock_cred, mock_blob_service_client, capsys):

    # MOCKING SETUP
    mock_blob1 = Mock()
    mock_blob1.name = "Fichero prueba MSTeams.txt"
    mock_blob1.readall.return_value = b"Esto es un fichero de prueba para MSTeams"
    mock_blob_client = Mock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"Esto es un fichero de prueba para MSTeams"
    mock_container_client = MagicMock()
    mock_container_client.list_blobs.return_value = [mock_blob1]
    mock_container_client.get_blob_client.return_value = mock_blob_client
    mock_blob_service_client.return_value = mock_container_client
    

    # create temporary directory
    temp_dir = tempfile.mkdtemp()

    try:
        container_dest = Path(temp_dir)

        # RUN METHOD TO TEST
        blob = st._Blob(st._FS("predictland2", "PredictLand One", "developing", "plpipes-dev"), "\containers\oscar-container\Fichero prueba para MSTeams.txt", "oscar-container", "Fichero prueba para MSTeams.txt")
        blob._get(dir=temp_dir)

        blob_path = container_dest / "Fichero prueba para MSTeams.txt"

        # ASSERTIONS
        captured = capsys.readouterr()

        # check the file has been properly downloaded
        assert blob_path.exists()

        # check that the content of the file is correct
        assert blob_path.read_bytes() == b"Esto es un fichero de prueba para MSTeams" 
        
        # stdout assertion
        assert "Downloaded: Fichero prueba para MSTeams.txt" in captured.out
    
    finally:
        # close temporary directory
        shutil.rmtree(temp_dir)
    

@patch("plpipes.cloud.azure.storage.StorageManagementClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_fileshares_dir_ls(mock_cred, mock_storage_management_client, capsys):

    # MOCKING SETUP
    mock_fileshare = MagicMock(name="mock_fileshare")
    mock_fileshare.name = "oscar-fileshare"
    mock_storage_management_client.return_value.file_shares.list.return_value = [mock_fileshare]
    
    # RUN METHOD TO TEST
    fileshares_dir = st._FileSharesDir(st._FS("predictland2", "PredictLand One", "developing", "plpipes-dev"), "\fileshares")
    fileshares_dir.ls()

    # ASSERTIONS
    captured = capsys.readouterr()

    # check the fileshares directory has the correct fileshares
    assert fileshares_dir._fileshares == {"oscar-fileshare": st._FileShare}

    # stdout assertions
    assert "oscar-fileshare" in captured.out


@patch("plpipes.cloud.azure.storage.ShareClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_fileshare_ls(mock_cred, mock_share_client, capsys):

    # MOCKING SETUP
    mock_file1 = MagicMock(name="mock_file1")
    mock_file1.name = "bar"
    mock_file2 = MagicMock(name="mock_file2")
    mock_file2.name = "FirstBlob2.txt"
    mock_fileshare_client = MagicMock()
    mock_fileshare_client.list_directories_and_files.return_value = [mock_file1, mock_file2]
    mock_share_client.return_value = mock_fileshare_client

    # RUN METHOD TO TEST
    fileshare = st._FileShare(st._FS("predictland2", "PredictLand One", "developing", "plpipes-dev"), "\fileshares\oscar-fileshare", "oscar-fileshare")
    fileshare.ls() 

    # ASSERTIONS
    captured = capsys.readouterr()

    # check the fileshare has the correct files
    assert fileshare._files == {"bar": st._File, 
                                "FirstBlob2.txt": st._File}
    
    # stdout assertions
    assert "bar" in captured.out
    assert "FirstBlob2.txt" in captured.out


@patch("plpipes.cloud.azure.storage.ShareClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_fileshare_rget(mock_cred, mock_share_client, capsys):
    
    # MOCKING SETUP
    mock_file1 = Mock()
    mock_file1.name = "FirstBlob2.txt"
    mock_file1.readall.return_value = b"Segundo blob prueba"
    mock_file2 = create_mock_directory(name="bar", files=["Hello2.txt"])
    mock_file_client = Mock()
    mock_file_client.download_file.return_value.readall.return_value = b"Segundo blob prueba"
    mock_fileshare_client = MagicMock()
    mock_fileshare_client.list_directories_and_files.return_value = [mock_file1, mock_file2]
    mock_fileshare_client.get_file_client.return_value = mock_file_client

    mock_share_client.return_value = mock_fileshare_client 

    

    # create temporary directory
    temp_dir = tempfile.mkdtemp()

    container_dest = Path(temp_dir)

    # RUN METHOD TO TEST
    fileshare = st._FileShare(st._FS("predictland2", "PredictLand One", "developing", "plpipes-dev"), "\fileshares\oscar-fileshare", "oscar-fileshare")
    fileshare._rget(dir=temp_dir)

    file_path = container_dest / "oscar-fileshare/FirstBlob2.txt"

    # ASSERTIONS
    captured = capsys.readouterr()

    # check the file has been properly downloaded
    assert file_path.exists()

    # check the content of the file is correct
    assert file_path.read_bytes() == b"Segundo blob prueba" 

    # stdout assertions
    assert "Downloaded: FirstBlob2.txt" in captured.out


@patch("plpipes.cloud.azure.storage.ShareClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_file_get(mock_cred, mock_share_client, capsys):

    # MOCKING SETUP
    mock_file1 = Mock()
    mock_file1.name = "FirstBlob2.txt"
    mock_file1.readall.return_value = b"Segundo blob prueba"
    mock_file_client = Mock()
    mock_file_client.download_file.return_value.readall.return_value = b"Segundo blob prueba"
    mock_fileshare_client = MagicMock()
    mock_fileshare_client.list_directories_and_files.return_value = [mock_file1]
    mock_fileshare_client.get_file_client.return_value = mock_file_client

    mock_share_client.return_value = mock_fileshare_client
    
    
    temp_dir = tempfile.mkdtemp()

    # create teporary directory
    try:
        container_dest = Path(temp_dir)

        # RUN METHOD TO TEST
        file = st._File(st._FS("predictland2", "PredictLand One", "developing", "plpipes-dev"), "\fileshares\oscar-fileshare\FirstBlob2.txt", "oscar-fileshare", "FirstBlob2.txt")
        file._get(dir=temp_dir)

        file_path = container_dest / "FirstBlob2.txt"

        # ASSERTIONS
        captured = capsys.readouterr()

        # check the file has been properly downloaded
        assert file_path.exists()

        # check the content of the file is correct
        assert file_path.read_bytes() == b"Segundo blob prueba"

        # stdout assertions
        assert "Downloaded: FirstBlob2.txt" in captured.out
    
    finally:
        # close temporary directory
        shutil.rmtree(temp_dir)


@patch("plpipes.cloud.azure.storage.BlobServiceClient")
@patch("plpipes.cloud.azure.storage._cred")
def test_container_rget(mock_cred, mock_blob_service_client, capsys):

    # MOCKING SETUP
    mock_blob1 = Mock()
    mock_blob1.name = "Fichero prueba MSTeams.txt"
    mock_blob1.readall.return_value = b"Esto es un fichero de prueba para MSTeams"
    mock_blob2 = create_mock_directory(name="foo-dir", files=["Fichero prueba MSTeams.txt", "First Blob.txt"])
    mock_blob_client = Mock()
    mock_blob_client.download_blob.return_value.readall.return_value = b"Esto es un fichero de prueba para MSTeams"
    mock_blob_service_client.return_value.get_container_client.return_value.list_blobs.return_value = [mock_blob1]


    # create temporary directory
    temp_dir = tempfile.mkdtemp()

    container_dest = Path(temp_dir)

    # RUN METHOD TO TEST
    container = st._Container(st._FS("predictland2", "PredictLand One", "developing", "plpipes-dev"), "\containers\oscar-container", "oscar-container")
    container._rget(dir=temp_dir)

    blob_path = container_dest / "oscar-container/Fichero prueba MSTeams.txt"

    # ASSERTIONS
    captured = capsys.readouterr()

    # check the file has been properly downloaded
    assert blob_path.exists()

    # check the content of the file is correct
    assert blob_path.read_bytes() == b"Esto es un fichero de prueba para MSTeams" 

    # stdout assertions
    assert "Downloaded: Fichero prueba MSTeams.txt" in captured.out
