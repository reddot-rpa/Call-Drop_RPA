# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(['workflow\\send_sms.py'],
             pathex=['E:\\Sunjid_development\\Call-Drop-RPA'],
             binaries=[],
             datas=[('E:\\Rayhan_development\\Call-Drop-RPA\\venvx\\Lib\\site-packages\\autoit\\lib\\AutoItX3_x64.dll', 'autoit\\lib')],
             hiddenimports=[],
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)

exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,  
          [],
          name='call_drop_rebate_send_sms',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None )
