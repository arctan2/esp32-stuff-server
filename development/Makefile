scan:
	sudo losetup --partscan --find --show fat32.img
mkfs_fat:
	sudo mkfs.fat -F 32 -n TESTVOL $(LOOP)
